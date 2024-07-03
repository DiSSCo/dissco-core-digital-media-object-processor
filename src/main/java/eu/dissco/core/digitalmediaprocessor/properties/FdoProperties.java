package eu.dissco.core.digitalmediaprocessor.properties;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Data
@Validated
@ConfigurationProperties("fdo")
public class FdoProperties {

  @NotBlank
  private String type = "https://hdl.handle.net/21.T11148/bbad8c4e101e8af01115";

  @NotBlank
  private String issuedForAgent = "https://ror.org/0566bfb96";

}
