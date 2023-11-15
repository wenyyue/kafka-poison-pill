package org.example.withoutschemaregistry.consumer;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.withoutschemaregistry.deserializer.AvroDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;


public class ConsumerWithoutRegistry {
    public void startConsume() {
        Properties props = new Properties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // Use customised avro deserializer.
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AvroDeserializer.class);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "c1");

        KafkaConsumer<String, GenericRecord> orderConsumer = new KafkaConsumer<>(props);
        orderConsumer.subscribe(Collections.singletonList("avro_test"));
        submitConsumer(orderConsumer);
    }

    public void submitConsumer(org.apache.kafka.clients.consumer.KafkaConsumer<String, GenericRecord> consumer) {
        while (true) {
            try {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(2000));
                consume(records);
            } catch (Exception e) {
                System.out.println("kafka consumer error: " + e.getMessage());
            } finally {
                consumer.commitSync();
            }
        }
    }

    private void consume(ConsumerRecords<String, GenericRecord> consumerRecords) {
        int size = consumerRecords.count();
        if (size == 0) {
            return;
        }
        for (ConsumerRecord<String, GenericRecord> record : consumerRecords) {
            GenericRecord datum = record.value();
            System.out.println(datum.get("content"));
        }
    }
}
