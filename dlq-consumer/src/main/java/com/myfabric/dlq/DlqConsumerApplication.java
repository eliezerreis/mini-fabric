package com.myfabric.dlq;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class DlqConsumerApplication {
    public static void main(String[] args) {
        SpringApplication.run(DlqConsumerApplication.class, args);
    }
}
