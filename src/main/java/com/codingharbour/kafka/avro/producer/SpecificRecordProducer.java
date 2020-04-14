package com.codingharbour.kafka.avro.producer;

import com.codingharbour.avro.SimpleMessage;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;

public class SpecificRecordProducer {

    public static void main(String[] args) {
        SpecificRecordProducer specificRecordProducer = new SpecificRecordProducer();
        specificRecordProducer.writeMessage();
    }

    public void writeMessage() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        Producer<String, SpecificRecord> producer = new KafkaProducer<>(properties);

        //create the specific record
        SimpleMessage simpleMessage = new SimpleMessage();
        simpleMessage.setContent("Hello world");
        simpleMessage.setDateTime(Instant.now().toString());

        ProducerRecord<String, SpecificRecord> record = new ProducerRecord<>("avro-topic", null, simpleMessage);

        producer.send(record);
        //ensures record is sent before closing the producer
        producer.flush();

        producer.close();
    }
}
