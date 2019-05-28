package com.demo.configuration;

import com.google.pubsub.v1.PubsubMessage;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.gcp.pubsub.PubSubAdmin;
import org.springframework.cloud.gcp.pubsub.core.PubSubTemplate;
import org.springframework.cloud.gcp.pubsub.support.BasicAcknowledgeablePubsubMessage;

import javax.annotation.PostConstruct;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Extend this class and override a few methods to benefit from retry and error topics.
 */
@Slf4j
public abstract class AbstractVanillaPubSubListenerWithRetry {

    private static final String FAILED_SUFFIX = "-failed";
    private static final String RETRY_SUFFIX = "-retry";

    private static final String RETRY_NUMBER = "retry-number";
    private static final int MAX_RETRY_NUMBER = 2;

    private static final long RETRY_DELAY_IN_MS = 3_000;

    @Autowired
    private PubSubTemplate pubSubTemplate;

    @Autowired
    private PubSubAdmin pubSubAdmin;

    /**
     * Topic name.
     */
    public abstract String getTopicName();

    /**
     * Subscription name.
     */
    public abstract String getSubscriptionName();

    /**
     * Behavior when receiving a Message from pub sub.
     * Any Exception thrown will result in the message being reattempted.
     */
    public abstract void receiveMessage(BasicAcknowledgeablePubsubMessage message);

    /**
     * I personnally like creating my topic / subscriptions programmatically.
     * You can decide not to but may have to change topic / subscriptions names.
     */
    @PostConstruct
    public void init() {

        initTopicsAndSubscriptions();

        listenToPubSubSubscription(getSubscriptionName());
        listenToPubSubSubscription(getRetrySubscriptionName());
    }

    /**
     * Name of the retry subscription.
     * It will be created by default but you can remove the subscription creation and override this if needed.
     */
    protected String getRetrySubscriptionName() {
        return getTopicName() + RETRY_SUFFIX;
    }

    /**
     * Name of the retry topic.
     * It will be created by default but you can remove the topic creation and override this if needed.
     */
    protected String getRetryTopicName() {
        return getTopicName() + RETRY_SUFFIX;
    }

    /**
     * Name of the failed topic.
     * It will be created by default but you can remove the topic creation and override this if needed.
     */
    protected String getFailedTopicName() {
        return getTopicName() + FAILED_SUFFIX;
    }

    /**
     * Default number of time a message will be retried before being discarded.
     * Override if needed.
     */
    protected int getMaxRetryNumber() { return MAX_RETRY_NUMBER; }

    /**
     * Default delay before a message will be retried.
     * Override if needed.
     */
    protected long getRetryDelayInMs() { return RETRY_DELAY_IN_MS; }

    private void createTopic(String topicName) {
        if (pubSubAdmin.getTopic(topicName) != null) {
            log.info("Topic {} already exists", topicName);
        } else {
            pubSubAdmin.createTopic(topicName);
            log.info("Topic {} created", topicName);
        }
    }

    private void createSubscription(String subscriptionName, String topicName) {
        if (pubSubAdmin.getSubscription(subscriptionName) != null) {
            log.info("Subscription {} already exists", subscriptionName);
        } else {
            pubSubAdmin.createSubscription(subscriptionName, topicName);
            log.info("Subscription {} created with topic {}", subscriptionName, topicName);
        }
    }

    private void initTopicsAndSubscriptions() {
        createTopic(getTopicName());
        createTopic(getRetryTopicName());
        createTopic(getFailedTopicName());
        createSubscription(getSubscriptionName(), getTopicName());
        createSubscription(getRetrySubscriptionName(), getRetryTopicName());
    }

    private void listenToPubSubSubscription(String subscriptionName) {
        pubSubTemplate.subscribe(subscriptionName, message -> {
            try {
                receiveMessage(message);
            } catch (RuntimeException ex) {
                failedMessage(message);
            }
        });
    }

    private void failedMessage(BasicAcknowledgeablePubsubMessage message) {

        val originalMessage = message.getPubsubMessage();
        val originalPayload = originalMessage.getData();

        val currentRetryNumber = getRetryNumber(originalMessage);
        log.debug("entering error channel, retry {}", currentRetryNumber);

        if (currentRetryNumber > getMaxRetryNumber()) {
            log.error("Max number of retry reached, publishing to failed topic {}", getFailedTopicName());
            pubSubTemplate.publish(getFailedTopicName(), originalPayload);
        } else {
            val newRetryNumber = currentRetryNumber + 1;
            log.warn("Retry {}, publishing to retry topic {}", newRetryNumber, getRetryTopicName());

            val timer = new Timer();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    pubSubTemplate.publish(getRetryTopicName(), originalPayload, Map.of(RETRY_NUMBER, Integer.toString(newRetryNumber)));
                }
            }, getRetryDelayInMs());
        }

        message.ack();
    }

    private int getRetryNumber(PubsubMessage message) {
        return Integer.parseInt(message.getAttributesOrDefault(RETRY_NUMBER, "0"));
    }
}
