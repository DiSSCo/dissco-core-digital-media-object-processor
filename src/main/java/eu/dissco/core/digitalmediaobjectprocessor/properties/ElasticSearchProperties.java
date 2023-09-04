package eu.dissco.core.digitalmediaobjectprocessor.properties;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Data
@Validated
@ConfigurationProperties("elasticsearch")
public class ElasticSearchProperties {

  @NotBlank
  private String hostname;

  @Positive
  private int port;

  @NotBlank
  private String indexName;

  private String username;

  private String password;
}
