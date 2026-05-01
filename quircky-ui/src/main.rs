use quircky::{Event, NickPrefix, Quircky};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().init();

    let client = Quircky::builder("blahblah123")
        .connect("irc.libera.chat:6667")
        .await?;

    let (handle, mut events) = client.run();

    while let Some(event) = events.recv().await {
        if let Event::Ready { .. } = event {
            handle.join("#rust").await?;
            break;
        }
    }

    while let Some(event) = events.recv().await {
        match event {
            Event::Message { from, target, text } => println!("<{from} -> {target}> {text}"),
            Event::Joined { who, channel } => println!("* {who} joined {channel}"),
            Event::Part {
                who,
                channel,
                reason: Some(reason),
            } => println!("* {who} left {channel} ({reason})"),
            Event::Part {
                who,
                channel,
                reason: None,
            } => println!("* {who} left {channel}"),
            Event::Quit {
                who,
                reason: Some(reason),
            } => println!("* {who} quit ({reason})"),
            Event::Quit { who, reason: None } => println!("* {who} quit"),
            Event::NickChange { old, new } => println!("* {old} is now known as {new}"),
            Event::Notice { from, target, text } => println!("[{from} -> {target}] {text}"),
            Event::Ping => println!("pinged"),
            Event::Error(msg) => println!("error: {msg}"),
            Event::Ready { nick, message } => println!("* ready as {nick}: {message}"),
            Event::NickInUse { attempted } => println!("* nick {attempted} is already in use"),
            Event::NickError { attempted, reason } => {
                println!("* nick {attempted} error: {reason}");
            }
            Event::Topic { channel, topic } => println!("* topic for {channel}: {topic}"),
            Event::NamesReply { channel, names } => {
                let formatted = names
                    .iter()
                    .map(|(nick, prefix)| match prefix {
                        NickPrefix::Op => format!("@{nick}"),
                        NickPrefix::Voice => format!("+{nick}"),
                        NickPrefix::None => nick.clone(),
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                println!("* users in {channel}: {formatted}");
            }
            Event::NamesEnd { channel } => println!("* end of names for {channel}"),
            Event::Motd(line) => println!("motd: {line}"),
            Event::MotdEnd => println!("* end of motd"),
        }
    }

    Ok(())
}
