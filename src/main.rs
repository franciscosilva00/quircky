use tokio::{
    io::{self, AsyncBufReadExt, AsyncWriteExt},
    net,
    sync::mpsc,
};

enum Command {
    Nick(String),
    Join(String),
    Privmsg { target: String, text: String },
    Pong(String),
}

impl Command {
    fn into_raw(self) -> String {
        match self {
            Self::Nick(nick) => format!("NICK {nick}\r\n"),
            Self::Join(channel) => format!("JOIN {channel}\r\n"),
            Self::Privmsg { target, text } => format!("PRIVMSG {target} :{text}\r\n"),
            Self::Pong(token) => format!("PONG :{token}\r\n"),
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
        channel: String,
    },
    Unknown(String),
}

impl Event {
    // TODO: better parsing
    fn parse(line: &str) -> Self {
        let parts: Vec<&str> = line.splitn(4, ' ').collect();

        if parts.len() >= 4 && parts[1] == "PRIVMSG" {
            let from = parts[0]
                .trim_start_matches(':')
                .split('!')
                .next()
                .unwrap_or("")
                .to_string();
            let target = parts[2].to_string();
            let text = parts[3].trim_start_matches(':').to_string();
            return Self::Message { from, target, text };
        }

        if parts.len() >= 3 && parts[1] == "JOIN" {
            let channel = parts[2].trim_start_matches(':').to_string();
            return Self::Joined { channel };
        }

        Self::Unknown(line.to_string())
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

        quircky.write_raw(format!("NICK {nick}\r\n")).await?;
        quircky
            .write_raw(format!("USER {nick} 0 * :{realname}\r\n"))
            .await?;

        Ok(quircky)
    }

    async fn write_raw(&mut self, line: String) -> anyhow::Result<()> {
        self.writer.write_all(line.as_bytes()).await?;

        Ok(())
    }

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

    pub async fn next_event(&mut self) -> anyhow::Result<Option<Event>> {
        loop {
            tokio::select! {
                line = self.reader.next_line() => {
                    let Some(line) = line? else {
                        return Ok(None);
                    };

                    if let Some(token) = line.strip_prefix("PING :") {
                        self.tx.send(Command::Pong(token.into())).await?;
                        continue;
                    }

                    return Ok(Some(Event::parse(&line)));
                }

                cmd = self.rx.recv() => {
                    let Some(cmd) = cmd else {
                        return Ok(None);
                    };

                    self.writer.write_all(cmd.into_raw().as_bytes()).await?;
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
            Event::Joined { channel } => println!("* joined {channel}"),
            Event::Unknown(line) => eprintln!("?? {line}"),
        }
    }

    Ok(())
}
