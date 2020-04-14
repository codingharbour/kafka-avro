package com.codingharbour.kafka.avro.consumer;

import com.codingharbour.avro.SimpleMessage;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class SpecificRecordConsumer {

    public static void main(String[] args) {
        SpecificRecordConsumer genericRecordConsumer = new SpecificRecordConsumer();
        genericRecordConsumer.readMessages();
    }

    public void readMessages() {
        //create kafka producer
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "specific-record-consumer-group");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);

        properties.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        KafkaConsumer<String, SimpleMessage> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Collections.singleton("avro-topic"));

        //poll the record from the topic
        while (true) {
            ConsumerRecords<String, SimpleMessage> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, SimpleMessage> record : records) {
                System.out.println("Message content: " + record.value().getContent());
                System.out.println("Message time: " + record.value().getDateTime());
            }
            consumer.commitAsync();
        }

    }
}
