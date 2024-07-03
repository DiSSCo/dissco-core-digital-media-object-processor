package eu.dissco.core.digitalmediaprocessor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableKafka
@EnableScheduling
@EnableCaching
@ConfigurationPropertiesScan
@SpringBootApplication
public class DigitalMediaProcessorApplication {

	public static void main(String[] args) {
		SpringApplication.run(DigitalMediaProcessorApplication.class, args);
	}

}
