package com.tgi.retail_sale.kafka;

import java.nio.charset.StandardCharsets;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

@Slf4j
@RequiredArgsConstructor
public abstract class DefaultProducer<T> {

  protected final StreamBridge streamBridge;

  protected boolean sendMessage(String topicName, T message, String key) {
    boolean isSuccess = false;
    try{

             isSuccess = streamBridge.send(
                      topicName,
                      MessageBuilder.withPayload(message)
//                .setHeader("correlation-id", correlationId)
                              .setHeader(KafkaHeaders.KEY, key.getBytes(StandardCharsets.UTF_8))
                              .build());
    } catch (Exception e) {
      // Fail
    }

    if (isSuccess) {
      log.info(
          "Successfully sent message to topic: {}, key: {}", topicName, key);
    } else {
      log.error("Failure sent message to topic: {}, key: {}", topicName, key);
    }
    return isSuccess;
  }
}
