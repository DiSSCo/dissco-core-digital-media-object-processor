package eu.dissco.core.digitalmediaobjectprocessor.properties;

import eu.dissco.core.digitalmediaobjectprocessor.Profiles;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Positive;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Profile;
import org.springframework.validation.annotation.Validated;


@Data
@Validated
@Profile(Profiles.KAFKA)
@ConfigurationProperties("kafka.consumer")
public class KafkaConsumerProperties {

  @NotBlank
  private String host;

  @NotBlank
  private String group;

  @NotBlank
  private String topic;

  @Positive
  private int batchSize = 500;

}
