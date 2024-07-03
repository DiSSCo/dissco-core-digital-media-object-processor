package eu.dissco.core.digitalmediaprocessor.configuration;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.time.Instant;
import java.util.Date;
import java.util.Random;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ApplicationConfiguration {

  public static final String DATE_STRING = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";

  @Bean
  public ObjectMapper objectMapper() {
    var mapper = new ObjectMapper().findAndRegisterModules();
    SimpleModule dateModule = new SimpleModule();
    dateModule.addSerializer(Instant.class, new InstantSerializer());
    dateModule.addDeserializer(Instant.class, new InstantDeserializer());
    dateModule.addDeserializer(Date.class, new DateDeserializer());
    dateModule.addSerializer(Date.class, new DateSerializer());
    mapper.registerModule(dateModule);
    mapper.setSerializationInclusion(Include.NON_NULL);
    return mapper;
  }

  @Bean
  public Random random() {
    return new Random();
  }

  @Bean
  public DocumentBuilder documentBuilder() throws ParserConfigurationException {
    var docFactory = DocumentBuilderFactory.newInstance();
    docFactory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
    return docFactory.newDocumentBuilder();
  }

  @Bean
  public TransformerFactory transformerFactory() throws TransformerConfigurationException {
    var factory = TransformerFactory.newInstance();
    factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
    factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_STYLESHEET, "");
    factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
    return factory;
  }

}
