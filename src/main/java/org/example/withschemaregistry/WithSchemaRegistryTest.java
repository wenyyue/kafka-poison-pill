package org.example.withschemaregistry;

import org.example.avro.SimpleMessage;
import org.example.withschemaregistry.consumer.KfkConsumer;
import org.example.withschemaregistry.producer.KfkProducer;

public class WithSchemaRegistryTest {
    public static void main(String[] args) {
//        KfkProducer producer = new KfkProducer();
//
//        SimpleMessage simpleMessage = SimpleMessage.newBuilder().setContent("xx小小").setDateTime("20231111").build();
//
//        producer.sendMessage(simpleMessage);
//
//        while (true) {
//
//        }

        KfkConsumer consumer = new KfkConsumer();
        consumer.startConsume();
    }
}
