package org.example;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.example.avro.SimpleMessage;
import org.example.witherrorhandling.consumer.ConsumerWithErrorHandling;
import org.example.withoutschemaregistry.consumer.ConsumerWithoutRegistry;
import org.example.withoutschemaregistry.producer.ProducerWithoutRegistry;
import org.example.withschemaregistry.consumer.ConsumerWithRegistry;
import org.example.withschemaregistry.producer.ProducerWithRegistry;

/**
 * Unit test for simple App.
 */
public class AppTest extends TestCase {
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite( AppTest.class );
    }

    /**
     * Produce messages without schema registry.
     * In this case, we will use our own defined avro serializer.
     */
    public void testProduceMsgWithoutRegistry() {
        SimpleMessage simpleMessage = SimpleMessage
                .newBuilder()
                .setContent("test22")
                .setDateTime("20231111")
                .build();

        ProducerWithoutRegistry producer = new ProducerWithoutRegistry();
        producer.sendMessage(simpleMessage);

        while (true) {

        }
    }

    /**
     * Consume messages without schema registry.
     * In this case, we will use our own defined deserializer,
     * which demonstrates how the deserializer processes an inappropriate message with try-catch mechanisms.
     */
    public void testConsumeMsgWithoutRegistry() {
        ConsumerWithoutRegistry consumer = new ConsumerWithoutRegistry();
        consumer.startConsume();
    }

    /**
     * Produce messages with schema registry.
     */
    public void testProduceMsgWithRegistry() {
        SimpleMessage simpleMessage = SimpleMessage
                .newBuilder()
                .setContent("test123")
                .setDateTime("20231111")
                .build();

        ProducerWithRegistry producer = new ProducerWithRegistry();
        producer.sendMessage(simpleMessage);

        while (true) {

        }
    }

    /**
     * Consume messages with schema registry, in this case,
     * it will encounter kafka-poison-pill with KafkaAvroDeserializer when consume an inappropriate message.
     */
    public void testConsumeMsgWithRegistry() {
        ConsumerWithRegistry consumer = new ConsumerWithRegistry();
        consumer.startConsume();
    }

    /**
     * Consume messages with deserialization error handling, in this case,
     * we will solve kafka-poison-pill problem when using KafkaAvroDeserializer by Error Handling mechanisms.
     */
    public void testConsumeMsgWithErrorHandling() {
        ConsumerWithErrorHandling consumer = new ConsumerWithErrorHandling();
        consumer.startConsume();
    }
}
