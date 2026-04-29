use irc_proto::{Command as IrcCommand, Message, Prefix};
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
}

fn nick_from_prefix(prefix: Option<Prefix>) -> Option<String> {
    match prefix {
        Some(Prefix::Nickname(nick, _, _)) => Some(nick),
        _ => None,
    }
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

    pub fn username(mut self, username: impl Into<String>) -> Self {
        self.username = Some(username.into());
        self
    }

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

                    // ping handled internally
                    if let IrcCommand::PING(token, _) = msg.command {
                        let pong: Message = IrcCommand::PONG(token, None).into();
                        self.writer.write_all(pong.to_string().as_bytes()).await?;

                        continue;
                    }

                    match Event::from_message(msg) {
                        Some(event) => return Ok(Some(event)),
                        None => trace!(raw = %line, "ignored message"),
                    }
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let client = Quircky::builder("blahblah123")
        .connect("irc.libera.chat:6667")
        .await?;

    let (handle, mut events) = client.run();

    handle.join("#rust").await?;

    while let Some(event) = events.recv().await {
        match event {
            Event::Message { from, target, text } => println!("<{from} -> {target}> {text}"),
            Event::Joined { who, channel } => println!("* {who} joined {channel}"),
        }
    }

    Ok(())
}
