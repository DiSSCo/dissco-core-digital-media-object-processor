package eu.dissco.core.digitalmediaprocessor.properties;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Data
@Validated
@ConfigurationProperties("application")
public class ApplicationProperties {

  @NotBlank
  private String name = "DiSSCo Digital Media Processing Service";

  @NotBlank
  private String pid = "https://doi.org/10.5281/zenodo.14383386";

  @NotBlank
  private String createUpdateTombstoneEventType = "https://doi.org/21.T11148/d7570227982f70256af3";

}
