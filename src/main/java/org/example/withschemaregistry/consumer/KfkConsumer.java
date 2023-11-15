package org.example.withschemaregistry.consumer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;


public class KfkConsumer {
//    public static final Logger logger = LoggerFactory.getLogger(KfkConsumer.class);

    public void startConsume() {
        Properties props = new Properties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        // 使用KafkaAvroDeserializer反序列化器必须指定 schema registry地址
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
        long start = System.currentTimeMillis();
        //这里控制每次拿到多少批量数据的参数配置是: max.poll.records
        int size = consumerRecords.count();
        if (size == 0) {
            return;
        }
//        logger.info("消息进来了{}条",size);
        //具体操作的地方
        for (ConsumerRecord<String, GenericRecord> record : consumerRecords) {
            String idempotent = "kafka_idempotent_" + record.topic() + "_" + record.partition() + "_" + record.offset();


            GenericRecord datum = record.value();
            System.out.println(datum.get("content"));
//            ByteBuffer buffer = (ByteBuffer) datum.get(LogEntry.EVENT);
//            AbstractEvent event = (AbstractEvent) persistenceHelper.fromJsonBytes(buffer.array(), AbstractEvent.class);
//            event.setSeq((Long) datum.get(LogEntry.SEQ));
//            event.setTimestamp((Long) datum.get(LogEntry.TIMESTAMP));
//            MDC.put("trace_id", BizSequenceUtils.generateBizSeqNo("OMS"));
//            handleEvent(event);
        }
        long end = System.currentTimeMillis();
//        logger.info("+++++++++++++++kafka transaction polling returned batch of {" + consumerRecords.count() + "} messages, cost:{" + (end - start) + "} ms");
    }
}
