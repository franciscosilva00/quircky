use irc_proto::{Command as IrcCommand, Message, Prefix, Response};
use tokio::{
    io::{self, AsyncBufReadExt, AsyncWriteExt},
    net,
    sync::mpsc,
};
use tracing::{info, trace, warn};

enum Command {
    Nick(String),
    Join(String),
    Privmsg { target: String, text: String },
}

impl Command {
    fn into_message(self) -> Message {
        match self {
            Self::Nick(nick) => IrcCommand::NICK(nick).into(),
            Self::Join(channel) => IrcCommand::JOIN(channel, None, None).into(),
            Self::Privmsg { target, text } => IrcCommand::PRIVMSG(target, text).into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NickPrefix {
    Op,
    Voice,
    None,
}

impl NickPrefix {
    const fn from_char(c: char) -> Self {
        match c {
            '@' => Self::Op,
            '+' => Self::Voice,
            _ => Self::None,
        }
    }
}

pub enum Event {
    Message {
        from: String,
        target: String,
        text: String,
    },
    Joined {
        who: String,
        channel: String,
    },
    Part {
        who: String,
        channel: String,
        reason: Option<String>,
    },
    Quit {
        who: String,
        reason: Option<String>,
    },
    NickChange {
        old: String,
        new: String,
    },
    Notice {
        from: String,
        target: String,
        text: String,
    },
    Ping,
    Error(String),

    Ready {
        nick: String,
        message: String,
    },
    NickInUse {
        attempted: String,
    },
    NickError {
        attempted: String,
        reason: String,
    },
    Topic {
        channel: String,
        topic: String,
    },
    NamesReply {
        channel: String,
        names: Vec<(String, NickPrefix)>,
    },
    NamesEnd {
        channel: String,
    },
    Motd(String),
    MotdEnd,
}

fn nick_from_prefix(prefix: Option<Prefix>) -> Option<String> {
    match prefix {
        Some(Prefix::Nickname(nick, _, _)) => Some(nick),
        _ => None,
    }
}

fn parse_names(raw: &str) -> Vec<(String, NickPrefix)> {
    raw.split_whitespace()
        .map(|s| {
            let first = s.chars().next().unwrap_or(' ');
            let prefix = NickPrefix::from_char(first);
            let nick = if prefix == NickPrefix::None {
                s.to_string()
            } else {
                s[1..].to_string()
            };
            (nick, prefix)
        })
        .collect()
}

impl Event {
    fn from_message(msg: Message) -> Option<Self> {
        match msg.command {
            IrcCommand::PRIVMSG(target, text) => Some(Self::Message {
                from: nick_from_prefix(msg.prefix)?,
                target,
                text,
            }),
            IrcCommand::JOIN(channel, _, _) => Some(Self::Joined {
                who: nick_from_prefix(msg.prefix)?,
                channel,
            }),
            IrcCommand::PART(channel, reason) => Some(Self::Part {
                who: nick_from_prefix(msg.prefix)?,
                channel,
                reason,
            }),
            IrcCommand::QUIT(reason) => Some(Self::Quit {
                who: nick_from_prefix(msg.prefix)?,
                reason,
            }),
            IrcCommand::NICK(new) => Some(Self::NickChange {
                old: nick_from_prefix(msg.prefix)?,
                new,
            }),
            IrcCommand::NOTICE(target, text) => Some(Self::Notice {
                from: nick_from_prefix(msg.prefix).unwrap_or_else(|| "server".into()),
                target,
                text,
            }),
            IrcCommand::PING(_, _) => Some(Self::Ping),
            IrcCommand::ERROR(msg) => Some(Self::Error(msg)),

            IrcCommand::Response(code, params) => Self::from_numeric(code, params),

            _ => None,
        }
    }

    fn from_numeric(code: Response, mut params: Vec<String>) -> Option<Self> {
        match code {
            Response::RPL_WELCOME => Some(Self::Ready {
                nick: params.first()?.clone(),
                message: params.into_iter().last()?,
            }),

            Response::RPL_TOPIC => {
                let topic = params.pop()?;
                let channel = params.into_iter().nth(1)?;
                Some(Self::Topic { channel, topic })
            }

            Response::RPL_NAMREPLY => {
                let names_raw = params.pop()?;
                let channel = params.into_iter().last()?;
                Some(Self::NamesReply {
                    channel,
                    names: parse_names(&names_raw),
                })
            }

            Response::RPL_ENDOFNAMES => {
                let channel = params.into_iter().nth(1)?;
                Some(Self::NamesEnd { channel })
            }

            Response::RPL_MOTD => {
                let text = params.into_iter().last()?;
                Some(Self::Motd(text))
            }

            Response::RPL_ENDOFMOTD => Some(Self::MotdEnd),

            Response::ERR_NICKNAMEINUSE => {
                let attempted = params.into_iter().nth(1)?;
                Some(Self::NickInUse { attempted })
            }

            Response::ERR_NONICKNAMEGIVEN | Response::ERR_ERRONEOUSNICKNAME => {
                let mut it = params.into_iter();
                let attempted = it.nth(1).unwrap_or_default();
                let reason = it.last().unwrap_or_default();
                Some(Self::NickError { attempted, reason })
            }

            _ => None,
        }
    }
}

#[derive(Clone)]
pub struct ClientHandle {
    tx: mpsc::Sender<Command>,
}

impl ClientHandle {
    pub async fn nick(&self, nick: &str) -> anyhow::Result<()> {
        self.tx.send(Command::Nick(nick.into())).await?;

        Ok(())
    }

    pub async fn join(&self, channel: &str) -> anyhow::Result<()> {
        self.tx.send(Command::Join(channel.into())).await?;

        Ok(())
    }

    pub async fn privmsg(&self, target: &str, text: &str) -> anyhow::Result<()> {
        self.tx
            .send(Command::Privmsg {
                target: target.into(),
                text: text.into(),
            })
            .await?;

        Ok(())
    }
}

type Reader = io::Lines<io::BufReader<io::ReadHalf<net::TcpStream>>>;
type Writer = io::WriteHalf<net::TcpStream>;

pub struct QuirckyBuilder {
    nick: String,
    username: Option<String>,
    realname: Option<String>,
}

impl QuirckyBuilder {
    pub fn new(nick: impl Into<String>) -> Self {
        Self {
            nick: nick.into(),
            username: None,
            realname: None,
        }
    }

    #[must_use]
    pub fn username(mut self, username: impl Into<String>) -> Self {
        self.username = Some(username.into());
        self
    }

    #[must_use]
    pub fn realname(mut self, realname: impl Into<String>) -> Self {
        self.realname = Some(realname.into());
        self
    }

    pub async fn connect(self, addr: impl net::ToSocketAddrs) -> anyhow::Result<Quircky> {
        let username = self.username.unwrap_or_else(|| self.nick.clone());
        let realname = self.realname.unwrap_or_else(|| self.nick.clone());

        Quircky::connect_inner(addr, &self.nick, &username, &realname).await
    }
}

pub struct Quircky {
    reader: Reader,
    writer: Writer,

    tx: mpsc::Sender<Command>,
    rx: mpsc::Receiver<Command>,
}

impl Quircky {
    pub fn builder(nick: impl Into<String>) -> QuirckyBuilder {
        QuirckyBuilder::new(nick)
    }

    async fn connect_inner(
        addr: impl net::ToSocketAddrs,
        nick: &str,
        username: &str,
        realname: &str,
    ) -> anyhow::Result<Self> {
        info!("connecting...");
        let stream = net::TcpStream::connect(addr).await?;
        info!("connected, sending handshake");

        let (reader, writer) = io::split(stream);
        let reader = io::BufReader::new(reader).lines();
        let (tx, rx) = mpsc::channel(32);

        let mut quircky = Self {
            reader,
            writer,
            tx,
            rx,
        };

        quircky
            .write_message(IrcCommand::NICK(nick.into()).into())
            .await?;

        quircky
            .write_message(IrcCommand::USER(username.into(), "0".into(), realname.into()).into())
            .await?;

        Ok(quircky)
    }

    async fn write_message(&mut self, msg: Message) -> anyhow::Result<()> {
        trace!(raw = %msg, "sending message");
        self.writer.write_all(msg.to_string().as_bytes()).await?;

        Ok(())
    }

    #[must_use]
    pub fn run(mut self) -> (ClientHandle, mpsc::Receiver<Event>) {
        let handle = ClientHandle {
            tx: self.tx.clone(),
        };
        let (event_tx, event_rx) = mpsc::channel(32);

        tokio::spawn(async move {
            loop {
                match self.next_event().await {
                    Ok(Some(event)) => {
                        if event_tx.send(event).await.is_err() {
                            warn!("event receiver dropped, shutting down");
                            break;
                        }
                    }
                    Ok(None) => {
                        info!("connection closed");
                        break;
                    }
                    Err(e) => {
                        warn!(error = %e, "error in event loop");
                        break;
                    }
                }
            }
        });

        (handle, event_rx)
    }

    async fn next_event(&mut self) -> anyhow::Result<Option<Event>> {
        loop {
            tokio::select! {
                line = self.reader.next_line() => {
                    let Some(line) = line? else {
                        return Ok(None);
                    };

                    trace!(raw = %line, "received line");

                    let msg: Message = line.parse()?;

                    // ping handled internally, still surfaced to user
                    if let IrcCommand::PING(ref token, _) = msg.command {
                        let pong: Message = IrcCommand::PONG(token.clone(), None).into();
                        self.writer.write_all(pong.to_string().as_bytes()).await?;
                    }

                    if let Some(event) = Event::from_message(msg) {
                        return Ok(Some(event));
                    }

                    trace!(raw = %line, "ignored message");
                }

                cmd = self.rx.recv() => {
                    let Some(cmd) = cmd else {
                        return Ok(None);
                    };

                    self.write_message(cmd.into_message()).await?;
                }
            }
        }
    }
}
