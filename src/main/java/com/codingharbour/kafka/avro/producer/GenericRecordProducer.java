package com.codingharbour.kafka.avro.producer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;

public class GenericRecordProducer {

    public static void main(String[] args) {
        GenericRecordProducer genericRecordProducer = new GenericRecordProducer();
        genericRecordProducer.writeMessage();
    }

    public void writeMessage() {
        //create kafka producer
Properties properties = new Properties();
properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

Producer<String, GenericRecord> producer = new KafkaProducer<>(properties);

        //avro schema
        String simpleMessageSchema =
                "{" +
                "   \"type\": \"record\"," +
                "   \"name\": \"SimpleMessage\"," +
                "   \"namespace\": \"com.codingharbour.avro\"," +
                "   \"fields\": [" +
                "       {\"name\": \"content\", \"type\": \"string\", \"doc\": \"Message content\"}," +
                "       {\"name\": \"date_time\", \"type\": \"string\", \"doc\": \"Datetime when the message\"}" +
                "   ]" +
                "}";

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(simpleMessageSchema);

        //prepare the avro record
        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("content", "Hello world");
        avroRecord.put("date_time", Instant.now().toString());
        System.out.println(avroRecord);

        //prepare the kafka record
        ProducerRecord<String, GenericRecord> record = new ProducerRecord<>("avro-topic", null, avroRecord);

        producer.send(record);
        //ensures record is sent before closing the producer
        producer.flush();

        producer.close();
    }
}
