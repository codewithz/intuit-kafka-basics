package com.intuit;



import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {
        String bootstrapServer="127.0.0.1:9092";
        String topic="intuit_first_topic";
        //Create Producer Properties

        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // Create the Producer

        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(properties);

        //Create a ProducerRecord
        ProducerRecord<String,String> record=new ProducerRecord<>(topic,"Hello from Java Code");

        // Send the data -- Synchronous  Way
        producer.send(record);

        //FLush the Data
        producer.flush();
    }
}
