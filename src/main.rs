extern crate getopts;
extern crate notify;

use std::io::{BufReader, Read, BufRead, SeekFrom, Seek, stdin};
use std::{env, process};
use getopts::{Matches, Options};
use std::process::ExitCode;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use std::time::{Duration};
use chrono::{Local};
use std::fs::File;
use std::fs::OpenOptions;
use std::path::Path;
use std::error::Error;
use std::string::ToString;
use notify::{RecommendedWatcher, Watcher, RecursiveMode};
use std::sync::mpsc::channel;

const BUF_SIZE :usize = 1024;

fn main() -> ExitCode {
    let args: Vec<_> = std::env::args().collect();
    let program = args[0].clone();
    let mut opts = Options::new();
    opts.optopt("f", "follow", "kafka sent data as the file grows", "FILE");
    opts.optopt("i", "ip_address", "kafka ip_address", "");
    opts.optopt("p", "port_number", "kafka port", "");
    opts.optopt("t", "topic_name", "kafka topic", "");    
    opts.optflag("h", "help", "print help");
    let matches = match opts.parse(&args[1..]) {
        Ok(m) => { m }
        Err(f) => { panic!("{}", f.to_string()) }
    };
    if matches.opt_present("h") {   // || Check count of args < 5(9) 
        print_usage(&program, opts);
//        ExitCode::FAILURE
    }
    let file = matches.opt_str("f").unwrap().to_string();
    let host_kafka = matches.opt_str("i").unwrap().to_string();
    let port_kafka = matches.opt_str("p").unwrap().to_string();
    let topic_name = matches.opt_str("t").unwrap().to_string();

    let producer: BaseProducer = kafka_init(host_kafka, port_kafka);    // Connect to Kafka as producer
    
    tail_file(&file, 10, true, producer, topic_name);

    ExitCode::SUCCESS
    }

fn kafka_init(host_kafka: String, port_kafka: String) -> BaseProducer { //} Result<BaseProducer, KafkaError>  {
        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers",  host_kafka + ":" + &port_kafka) // "127.0.0.1:9092")
            .create()
            .expect("Producer creation error");
    //    return Ok(producer);
        println!("{} Kafka inited.", get_now());
        return producer;
}

fn kafka_send(producer: &BaseProducer, topic: String, message: String) -> &BaseProducer { //Result<BaseProducer, KafkaError> {
        let formated_message = format!("{} : {message}", get_now());

        producer.send(
            BaseRecord::to(&topic)                 
                .payload(&formated_message.replace("\n", ""))     // Exclude '\n' - Alik Message to Send without '\n'
                .key(""),
        ).expect("Failed to enqueue");
    
        // Poll at regular intervals to process all the asynchronous delivery events.
        for _ in 0..10 {
            producer.poll(Duration::from_millis(100));
        }
        // And/or flush the producer before dropping it.
        producer.flush(Duration::from_secs(1));

        return producer;
}

fn get_now() -> String {
        let local_datetime: String = Local::now().format("%Y-%m-%d %H:%M:%S%.9f").to_string();
        local_datetime
}

fn get_args() -> Result<Matches, String> {
        let args: Vec<String> = env::args().collect();
        let mut options = Options::new();
        options.optopt("f", "-follow", "output appended data as the file grows", "FOLLOW");
        options.optflag("i", "-ip_address", "kafka ip_address");
        options.optflag("p", "-port_number", "kafka port");
        options.optflag("t", "-topic_name", "kafka topic");
        options.optflag("h", "-help", "print help");
        let cmd_args = match options.parse(&args[1..]) {
            Ok(p) => p,
            Err(why) => panic!("Cannot parse command args :{}", why),
        };
    
        Ok(cmd_args)
}

fn print_usage(program: &str, opts: Options) {
    let brief = format!("Usage: {} FILE [options]", program);
    print!("{}", opts.usage(&brief));
}

fn tail_file(path: &String, count: u64, fflag: bool, producer: BaseProducer, topic: String){
    let file = match OpenOptions::new().read(true).open(path) {
        Err (why) => panic!("Cannot open file! file:{} cause:{}", path, Error::description(&why)),
        Ok(file) => file
    };
    let f_metadata = match file.metadata(){
        Err(why) => panic!("Cannot read file metadata :{}", Error::description(&why)),
        Ok(data) => data
    };
    let f_size = f_metadata.len();
    //println!("file size is {} bytes", f_size);
    if f_size == 0 {
        process::exit(0);
    }
    let mut reader = BufReader::new(file);

    let mut line_count = 0;
    // minus 2 byte for skip eof null byte.
    let mut current_pos = f_size - 2;
    let mut read_start = if (f_size -2) > BUF_SIZE as u64 {
        f_size - 2 - BUF_SIZE as u64
    }else{
        0
    };
    let mut buf = [0;BUF_SIZE];
    'outer: loop {
        match reader.seek(SeekFrom::Start(read_start)){
            Err(why) => panic!("Cannot move offset! offset:{} cause:{}", current_pos, Error::description(&why)),
            Ok(_) => current_pos
        };
        let b = match reader.read(&mut buf){
            Err(why) => panic!("Cannot read offset byte! offset:{} cause:{}", current_pos, Error::description(&why)),
            Ok(b) => b
        };
        for i in 0..b{
            if buf[b-(i+1)] == 0xA {
                line_count += 1;
            }
            // println!("{}, {}", line_count, i);
            if line_count == count {
                break 'outer;
            }
            current_pos -= 1;
            //println!("{}", current_pos);
            if current_pos <= 0 {
                current_pos = 0;
                break 'outer;
            }
        }
        read_start = if read_start > BUF_SIZE as u64 {
            read_start - BUF_SIZE as u64
        }else{
            0
        }
    }
    //println!("last pos :{}", current_pos);
    match reader.seek(SeekFrom::Start(current_pos)){
        Err(why) => panic!("Cannot read offset byte! offset:{} cause:{}", current_pos, Error::description(&why)),
        Ok(_) => current_pos
    };
    let mut buf_str = String::new();
    match reader.read_to_string(&mut buf_str){
        Err(why) => panic!("Cannot read offset byte! offset:{} cause:{}", current_pos, Error::description(&why)),
        Ok(_) => current_pos
    };

    if fflag {
        if cfg!(target_os = "windows") {
            println!("");
        }
        if let Err(why) = tail_file_follow(&mut reader, &path.clone(), f_size, &producer, &topic, &buf_str){
            panic!("Cannot follow file! file:{:?} cause:{}", reader.by_ref(), Error::description(&why))
        }
    }
}

fn tail_file_follow(reader: &mut BufReader<File>, spath: &String, file_size: u64, producer: &BaseProducer, topic: &String, message: &String)->notify::Result<()>{
    let (tx, rx) = channel();
    let mut watcher: RecommendedWatcher = Watcher::new(tx,Duration::from_secs(1))?;
    let path = Path::new(spath);
    watcher.watch(path, RecursiveMode::NonRecursive)?;
    let mut start_byte = file_size;
    let mut buf_str = String::new();
    loop{
        match rx.recv(){
            Err(e) => println!("watch error: {:?}", e),
            Ok(_) => {
                match reader.seek(SeekFrom::Start(start_byte)){
                    Err(why) => panic!("Cannot move offset! offset:{} cause:{}", start_byte, Error::description(&why)),
                    Ok(_) => start_byte
                };
                let read_byte = match reader.read_to_string(&mut buf_str){
                    Err(why) => panic!("Cannot read offset byte! offset:{} cause:{}",start_byte, Error::description(&why)),
                    Ok(b) => b
                };
                start_byte += read_byte as u64;
                if !buf_str.is_empty() { kafka_send(producer, topic.to_string(), buf_str.to_string()); }    // Alik Kafka Send if File change is real(not empty)

                buf_str.clear();
            }
        }
    }
}

fn print_result(message: &String, producer: &BaseProducer, topic: &String){
    kafka_send(producer, topic.to_string(), message.to_string());
    print!("{}", message);
}
