package eu.dissco.core.digitalmediaprocessor.configuration;

import com.jayway.jsonpath.Option;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JsonpathConfiguration {

  @Bean
  public com.jayway.jsonpath.Configuration jsonPathConfig() {
    return com.jayway.jsonpath.Configuration.builder()
        .options(Option.AS_PATH_LIST, Option.SUPPRESS_EXCEPTIONS, Option.ALWAYS_RETURN_LIST)
        .build();
  }

}
