use std::process::ExitCode;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use std::time::{Duration, SystemTime};
use chrono::{DateTime, Utc};
use clap::Parser;

// -- SYSTEM_LOG 127.0.0.1 9092 quickstart-events

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path to FileName
    #[arg(short, long)]
    file: String,

    /// Host IP Address
    #[arg(short, long)]
    kafka_host: String,

    /// Kafka port Number
    #[arg(short, long, default_value_t = 9094)]
    port: u16,

    /// Topic Name
    #[arg(short, long)]
    topic: String,

    /// Number of times to greet
    #[arg(short, long, default_value_t = 1)]
    count: u8,
}

fn main() -> ExitCode {
    let args = Args::parse();
    let file_path = args.file.to_owned();
    let host_kafka = args.kafka_host;
    let port_kafka = args.port.to_string();
    let topic_name= args.topic;
    let message = "Message"; // file_path.clone();

    let mut producer: BaseProducer = kafka_init(host_kafka, port_kafka);;
    producer = kafka_send(producer, topic_name, message.to_string());

    println!("Hello, world!");

    ExitCode::SUCCESS
}

fn kafka_init(host_kafka: String, port_kafka: String) -> BaseProducer { //} Result<BaseProducer, KafkaError>  {
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers",  host_kafka + ":" + &port_kafka) // "127.0.0.1:9092")
        .create()
        .expect("Producer creation error");
//    return Ok(producer);
    return producer;
}

fn kafka_send(producer: BaseProducer, topic: String, message: String) -> BaseProducer { //Result<BaseProducer, KafkaError> {
    let system_time = SystemTime::now();
    let datetime: DateTime<Utc> = system_time.into();
    let mut formated_datetime = datetime.format("%Y-%m-%d %H:%M:%S.%f").to_string().to_owned();
    formated_datetime.push_str(":");
    let formated_message = format!("{formated_datetime}{message}");

    producer.send(
        BaseRecord::to(&topic)                 // "quickstart-events")
            .payload(&formated_message)     // Message to Send
            .key("and this is a key"),
    ).expect("Failed to enqueue");

    // Poll at regular intervals to process all the asynchronous delivery events.
    for _ in 0..10 {
        producer.poll(Duration::from_millis(100));
    }

    // And/or flush the producer before dropping it.
    producer.flush(Duration::from_secs(1));
//    return Ok(producer);
    return producer;
}