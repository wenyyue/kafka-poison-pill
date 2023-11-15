package org.example.withschemaregistry.producer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.avro.SimpleMessage;

import java.util.Properties;

public class ProducerWithRegistry {
    private final KafkaProducer<String, SimpleMessage> kafkaProducer;

    public ProducerWithRegistry() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://127.0.0.1:8081");
        kafkaProducer = new KafkaProducer<>(props);
    }

    public void sendMessage(SimpleMessage message) {
        kafkaProducer.send(new ProducerRecord<>("avro_test", message),
                (metadata, exception) -> System.out.println(metadata.toString()));
    }
}
