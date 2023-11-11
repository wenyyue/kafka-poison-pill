[官网本地启动流程](https://docs.confluent.io/platform/7.5/installation/installing_cp/zip-tar.html#zk)
启动顺序
1. zookeeper
2. kafka
3. schema registry
4. control center

测试流程：
失败测试：
1. 使用自定义avro序列化器生产消息或使用命令行生产消息
2. 使用KafkaAvroDeserializer反序列化器消费消息
   由于KafkaAvroDeserializer反序列化器无法消费以上两种方式产生的消息，最终会失败且不断重复消费，造成毒丸问题

成功测试：
使用自定义编写的Kafka反序列化错误处理器，对毒丸问题进行处理