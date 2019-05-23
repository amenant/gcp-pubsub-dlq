package com.demo.configuration;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.cloud.gcp.pubsub.support.BasicAcknowledgeablePubsubMessage;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;

import static org.springframework.cloud.gcp.pubsub.support.GcpPubSubHeaders.ORIGINAL_MESSAGE;

@Configuration
@Slf4j
public class PubSubTopicListener extends AbstractPubSubListenerWithRetry {

    private static final String TEST_TOPIC = "test-topic";
    private static final String TEST_SUBSCRIPTION = "test-subscription";

    @Override
    public String getTopicName() {
        return TEST_TOPIC;
    }

    @Override
    public String getSubscriptionName() {
        return TEST_SUBSCRIPTION;
    }

    @Override
    public void receiveMessage(Message message) {
        val originalMessage = message.getHeaders().get(ORIGINAL_MESSAGE, BasicAcknowledgeablePubsubMessage.class);
        val content = new String((byte []) message.getPayload());
        if (content.startsWith("error")) {
            throw new RuntimeException("error " + content);
        }
        log.info("received message : {}", content);
        originalMessage.ack();
    }
}
