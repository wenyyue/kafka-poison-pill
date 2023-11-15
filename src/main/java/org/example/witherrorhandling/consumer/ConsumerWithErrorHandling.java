package org.example.witherrorhandling.consumer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.antidote.ErrorHandlingDeserializer;
import org.example.antidote.FailedDeserializationFunction;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerWithErrorHandling {
    public void startConsume() {
        Properties props = new Properties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // Use Error Handler to delegate real deserializer to resolver poison-pill problem.
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, KafkaAvroDeserializer.class);
        // Specify a function to process when deserialize with failure.
        props.put(ErrorHandlingDeserializer.VALUE_FUNCTION, FailedDeserializationFunction.class);
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://127.0.0.1:8081");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "c1");
        KafkaConsumer<String, GenericRecord> orderConsumer = new KafkaConsumer<>(props);

        orderConsumer.subscribe(Collections.singletonList("avro_test"));

        submitConsumer(orderConsumer);
    }

    public void submitConsumer(KafkaConsumer<String, GenericRecord> consumer) {
        while (true) {
            try {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(2000));
                consume(records);
            } catch (Exception e) {
                System.out.println("kafka consumer error: " + e.getMessage()  + "Exception: " + e);
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
