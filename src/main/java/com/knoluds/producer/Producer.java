package com.knoluds.producer;
import com.knoluds.models.User;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Producer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "com.knoluds.utils.UserSerializer");
        KafkaProducer<String, User> kafkaProducer = new KafkaProducer<>(props);

        String name[]={"Sandeep","Subham","Akash","Ali","Deepak","Aashu","Rohit","Muzakkir","Sadik","Akshit","Jatin"};

        try {
            for (int counter = 1; counter <= 10; counter++) {
                User user = new User(counter, name[counter], (int) (20 + (Math.random() * 40)), "B.Tech");
                kafkaProducer.send(
                        new ProducerRecord(
                                "user",
                                String.valueOf(user.getId()),
                                user));

                System.out.println(user);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            kafkaProducer.close();
        }
    }
}