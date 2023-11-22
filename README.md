# What is it
This is a project that demonstrates how to resolve **Kafka Poison Pill** problem.

# Kafka Poison Pill Definition
Refer to [confluent](https://www.confluent.io/blog/spring-kafka-can-your-kafka-consumers-handle-a-poison-pill/)

# Architecture
| package                    | Description                                                                |
|----------------------------|----------------------------------------------------------------------------|
| antidote                   | The custom deserializer to solve poison pill                               |
| avro                       | The object of kafka avro message                                           |
| witherrorhandling.consumer | The consumer that use custom deserializer which can handle corrupt record. |
| withschemaregistry         | Consume messages with schema registry                                      |
| withoutschemaregistry      | Consume messages without schema registry                                   |  

# Analysis
You can find all illustrations in class AppTest, which shows different kinds of consumer perform differently with deserializing strategies.
And the key to protect program from poison is catch the exception when deserializing messages using KafkaAvroDeserializer.
Let us deep dive into the `KafkaAvroDeserializer.consume()`, it will throw a `SerializationException` when build `DeserializationContext` with `getByteBuffer(playload)` to consume a message.
So we need to catch any exception to assure the correct running state of program.