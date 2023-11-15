package org.example.withoutschemaregistry.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.avro.SimpleMessage;
import org.example.withoutschemaregistry.serializer.AvroSerializer;

import java.util.Properties;

public class ProducerWithoutRegistry {
    private final KafkaProducer<String, SimpleMessage> kafkaProducer;

    public ProducerWithoutRegistry() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // Use customised avro serializer.
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroSerializer.class);
        kafkaProducer = new KafkaProducer<>(props);
    }

    public void sendMessage(SimpleMessage message) {
        kafkaProducer.send(new ProducerRecord<>("avro_test", message),
                (metadata, exception) -> System.out.println(metadata.toString()));
    }
}
