package com.demo.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.gcp.pubsub.core.PubSubTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Testing controller.
 * Will read / write messages from / to Pub Sub emulator.
 */
@RestController
public class TestPubSubController {

    @Autowired
    private PubSubTemplate pubSubTemplate;

    @GetMapping("/send")
    public void publishMessage(@RequestParam("message") String message) {
        pubSubTemplate.publish("test-topic", message);
    }

    @GetMapping("/read-dlq")
    public List<String> readDlq() {

        return pubSubTemplate.pull("test-subscription-failed", 50, true)
                .stream()
                .map(message -> {
                    message.ack();
                    return pubSubTemplate.getMessageConverter().fromPubSubMessage(message.getPubsubMessage(), String.class);
                })
                .collect(Collectors.toList());
    }
}
