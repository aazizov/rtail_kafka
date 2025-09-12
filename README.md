# rtail_kafka

## Rust Implementation of _tail_ - GNU_Linux utility with Kafka producer(publisher) possibility.
This utility follow(catch up) for any LOG file and send string by string changes to Kafka server.
For usage You need to insert follow keys :
- -f - path to catch file
- -i - kafka server ip_address
- -p - kafka server port
- -t - kafka server topic name for insert changes

Fo example, iif we have to send any saved changes in SYSTEM_LOG file to Kafka server, we can execute follow command :
- for Windows - **rtail_kafka.exe -f SYSTEM_LOG -i 192.168.0.100 -p 9092 -t system_changes**
- for Linux
- for Mac


