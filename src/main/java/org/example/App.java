package org.example;

import org.example.avro.SimpleMessage;
import org.example.withoutschemaregistry.consumer.KfkConsumer;
import org.example.withoutschemaregistry.producer.KfkProducer;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) {
//        KfkProducer producer = new KfkProducer();
//
//        SimpleMessage simpleMessage = SimpleMessage.newBuilder().setContent("最新的测试222").setDateTime("20231111").build();
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

class TestA {
    public void a() {
        String a = "123";

        try {
            if ("123".equals(a)) {

            }
        } catch (Exception e) {
            throw new IllegalArgumentException("");
        }
    }

    public void b() {
        a();
    }
}
