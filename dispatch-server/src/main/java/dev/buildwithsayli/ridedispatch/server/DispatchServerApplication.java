package dev.buildwithsayli.ridedispatch.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class DispatchServerApplication {
    public static void main(String[] args) {
        SpringApplication.run(DispatchServerApplication.class, args);
    }
}
