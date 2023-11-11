package org.example.withoutschemaregistry.producer;

import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.avro.SimpleMessage;
import org.example.withoutschemaregistry.serializer.AvroSerializer;

import java.util.Properties;

public class KfkProducer {
    private final KafkaProducer<String, SimpleMessage> kafkaProducer;

    public KfkProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // 使用自定义avro序列化器
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroSerializer.class);
//        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://127.0.0.1:8081");
        kafkaProducer = new KafkaProducer<>(props);
    }

    public void sendMessage(SimpleMessage message) {
        kafkaProducer.send(new ProducerRecord<>("avro_test", message), new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                System.out.println(metadata.toString());
            }
        });
    }
}
