该示例使用自定义的schema serializer与deserializer，通过测试测试可以得知，必须使用schema序列化后的消息才能被正常消费
否则会报序列化失败的错误。

测试方法：
- 使用 Java方法发送正常序列化后的消息，开启消费者可以正常消费
- 在命令行中直接发送消息，由于没有使用avro序列化，消费者消费时会报异常

注意点：
本示例使用的commit提交方式为手动提交，由于在反序列化器中补获了异常，所以可以正常提交。
需要对比一下使用Kafka官方提供的KafkaAvroDeserializer，由于没有捕获异常导致commit提交失败产生毒丸问题。

我们项目中使用的Avro对象其实并没有发挥中Avro的作用，因为定义的字段接收的是二进制字节数组，消费者消费后本质上还是通过Json反序列化的方式消费消息，脱裤子放屁

- 使用官方的序列化器和反序列化器测试
则必须使用schema registry（否则会抛出异常Missing required configuration "schema.registry.url" which has no default value）。效果相同，唯一不同的是当消费异常消息即非Avro序列化的消息，由于未捕获异常会导致commit提交失败。