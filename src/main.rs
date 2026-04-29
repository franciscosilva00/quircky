use irc_proto::{Command as IrcCommand, Message, Prefix};
use tokio::{
    io::{self, AsyncBufReadExt, AsyncWriteExt},
    net,
    sync::mpsc,
};

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

pub struct Quircky {
    reader: Reader,
    writer: Writer,

    tx: mpsc::Sender<Command>,
    rx: mpsc::Receiver<Command>,
}

impl Quircky {
    pub async fn connect(
        addr: impl net::ToSocketAddrs,
        nick: &str,
        realname: &str,
    ) -> anyhow::Result<Self> {
        let stream = net::TcpStream::connect(addr).await?;
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
            .write_message(IrcCommand::USER(nick.into(), "0".into(), realname.into()).into())
            .await?;

        Ok(quircky)
    }

    async fn write_message(&mut self, msg: Message) -> anyhow::Result<()> {
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
            while let Ok(Some(event)) = self.next_event().await {
                if event_tx.send(event).await.is_err() {
                    break;
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

                    let msg: Message = line.parse()?;

                    // ping handled internally
                    if let IrcCommand::PING(token, _) = msg.command {
                        let pong: Message = IrcCommand::PONG(token, None).into();
                        self.writer.write_all(pong.to_string().as_bytes()).await?;

                        continue;
                    }

                    if let Some(event) = Event::from_message(msg) {
                        return Ok(Some(event));
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
    let client = Quircky::connect("irc.libera.chat:6667", "blahblah123", "blahblah123").await?;
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
