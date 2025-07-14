package org.example.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.example.model.WeatherData;

@Slf4j
public class WeatherConsumer {
  private static final String TOPIC = "weather-topic";
  private static final String BOOTSTRAP_SERVERS = "kafka:9092,localhost:9092"; // Оба адреса через запятую
  private static final String GROUP_ID = "weather-consumer-group";
  private static final ObjectMapper objectMapper = new ObjectMapper();

  public static void main(String[] args) {
    KafkaConsumer<String, String> consumer = getStringStringKafkaConsumer();

    try (consumer) {
      consumer.subscribe(Collections.singletonList(TOPIC));
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        System.out.println("Closing consumer");
        consumer.close();
      }));
      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

        for (ConsumerRecord<String, String> record : records) {
          try {
            WeatherData weatherData = objectMapper.readValue(record.value(), WeatherData.class);
            log.info("Received weather data: {}, partition: {}, offset: {}",
                weatherData, record.partition(), record.offset());
          } catch (Exception e) {
            log.error("Error parsing message: {}", e.getMessage());
          }
        }
      }
    }
  }

  private static KafkaConsumer<String, String> getStringStringKafkaConsumer() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.CLIENT_DNS_LOOKUP_CONFIG, "use_all_dns_ips");
    props.put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, "1000");
    props.put(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, "10000");

    return new KafkaConsumer<>(props);
  }
}
