package org.example.producer;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Properties;
import java.util.Random;
import org.example.model.WeatherData;

@Slf4j
public class WeatherProducer {
  private static final String TOPIC = "weather-topic";
  private static final String BOOTSTRAP_SERVERS = "localhost:29092";
  private static final ObjectMapper objectMapper = new ObjectMapper();

  @SneakyThrows
  public static void main(String[] args) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    KafkaProducer<String, String> producer = new KafkaProducer<>(props);

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      System.out.println("Closing producer");
      producer.close();
    }));

    String[] conditions = {"солнечно", "облачно", "дождь"};
    Random random = new Random();

    while (true) {
      double temperature = 35 * random.nextDouble();
      temperature = Math.round(temperature * 10) / 10.0; // Округление до 1 знака после запятой
      String condition = conditions[random.nextInt(conditions.length)];

      WeatherData weatherData = new WeatherData(temperature, condition);

      try {
        String json = objectMapper.writeValueAsString(weatherData);
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, json);

        producer.send(record, (metadata, exception) -> {
          if (exception != null) {
            log.error("Error sending message: {}", exception.getMessage());
          } else {
            log.info("Sent message: {}, partition: {}, offset: {}", json, metadata.partition(), metadata.offset());
          }
        });

        Thread.sleep(2000);
      } catch (InterruptedException | JsonProcessingException e) {
        e.printStackTrace();
      }
    }
  }
}
