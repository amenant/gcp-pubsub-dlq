package com.demo.configuration;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.cloud.gcp.pubsub.support.BasicAcknowledgeablePubsubMessage;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class PubSubVanillaTopicListener extends AbstractVanillaPubSubListenerWithRetry {

    private static final String TEST_TOPIC = "test-vanilla-topic";
    private static final String TEST_SUBSCRIPTION = "test-vanilla-subscription";

    @Override
    public String getTopicName() {
        return TEST_TOPIC;
    }

    @Override
    public String getSubscriptionName() {
        return TEST_SUBSCRIPTION;
    }

    @Override
    public String getFailedTopicName() { return "test-topic-failed"; }

    @Override
    public void receiveMessage(BasicAcknowledgeablePubsubMessage message) {
        val content = message.getPubsubMessage().getData().toStringUtf8();
        if (content.startsWith("error")) {
            throw new RuntimeException("error " + content);
        }
        log.info("received message : {}", content);
        message.ack();
    }
}
