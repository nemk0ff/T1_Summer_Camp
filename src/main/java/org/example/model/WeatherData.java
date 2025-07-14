package org.example.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class WeatherData {
  private double temperature;
  private String condition;
  
  @Override
  public String toString() {
    return "WeatherData{" +
        "temperature=" + temperature +
        ", condition='" + condition + '\'' +
        '}';
  }
}
