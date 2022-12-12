package com.njit;

import com.njit.service.KafkaSender;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class KafkaDemoApplication {

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(KafkaDemoApplication.class,args);
        KafkaSender sender = context.getBean(KafkaSender.class);

        for (int i = 0; i < 10; i++) {
            sender.send();
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
