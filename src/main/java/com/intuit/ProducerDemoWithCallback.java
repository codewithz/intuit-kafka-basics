package com.intuit;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    public static void main(String[] args) {
        String bootstrapServer="127.0.0.1:9092";

        Logger logger= LoggerFactory.getLogger(ProducerDemoWithCallback.class);
        String topic ="intuit_third_topic";

        //Create Producer Properties

        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // Create the Producer

        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(properties);

        for (int i=1;i<=10;i++){
            //Create a Producer Record
            ProducerRecord<String,String> record=new ProducerRecord<>(topic,"Hello from Java Code: "+Integer.toString(i));

            // Send the data -- Asynchronous way

            producer.send(record, new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            // Execute everytime a record is successfully sent, if record sending fails, it will throw an exception

                            if(e==null){
                                //Record is succesfully sent
                                logger.info("Recieved new MetaData");
                                logger.info("Topic:"+recordMetadata.topic()+"\n"+
                                        "Partition:"+recordMetadata.partition()+"\n"+
                                        "Offset:"+recordMetadata.offset()+"\n"+
                                        "Timestamp:"+recordMetadata.timestamp()

                                );
                            }
                            else{
                                //Exception have occurred
                                logger.error("Error while producing :",e);
                            }
                        }
                    }
            );
        }

            //Flush the Data
            producer.flush();

            //Flush and CLose Producer
            producer.close();

    }
}
