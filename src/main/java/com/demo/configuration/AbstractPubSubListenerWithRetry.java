package com.demo.configuration;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.gcp.pubsub.PubSubAdmin;
import org.springframework.cloud.gcp.pubsub.core.PubSubTemplate;
import org.springframework.cloud.gcp.pubsub.integration.AckMode;
import org.springframework.cloud.gcp.pubsub.integration.inbound.PubSubInboundChannelAdapter;
import org.springframework.cloud.gcp.pubsub.support.BasicAcknowledgeablePubsubMessage;
import org.springframework.cloud.gcp.pubsub.support.GcpPubSubHeaders;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.integration.dsl.context.IntegrationFlowContext;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.ErrorMessage;

import javax.annotation.PostConstruct;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Extend this class and override a few methods to benefit from retry and error topics.
 */
@Slf4j
public abstract class AbstractPubSubListenerWithRetry {

    private static final String FAILED_SUFFIX = "-failed";
    private static final String RETRY_SUFFIX = "-retry";

    private static final String RETRY_NUMBER = "retry-number";
    private static final int MAX_RETRY_NUMBER = 2;

    private static final long RETRY_DELAY_IN_MS = 3_000;

    @Autowired
    private IntegrationFlowContext flowContext;

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
    public abstract void receiveMessage(Message message);

    /**
     * I personnally like creating my topic / subscriptions programmatically.
     * You can decide not to but may have to change topic / subscriptions names.
     */
    @PostConstruct
    public void init() {

        initTopicsAndSubscriptions();

        val inputChannel = MessageChannels.direct().get();
        val retryChannel = MessageChannels.direct().get();
        val errorChannel = MessageChannels.direct().get();

        listenToPubSubTopic(inputChannel, errorChannel, getSubscriptionName());
        listenToPubSubTopic(retryChannel, errorChannel, getRetrySubscriptionName());

        val inputFlow = IntegrationFlows
                .from(inputChannel)
                .handle(this::receiveMessage)
                .get();
        flowContext.registration(inputFlow).register();

        val retryFlow = IntegrationFlows
                .from(retryChannel)
                .handle(this::receiveMessage)
                .get();
        flowContext.registration(retryFlow).register();

        val errorFlow = IntegrationFlows
                .from(errorChannel)
                .handle(this::failedMessage)
                .get();
        flowContext.registration(errorFlow).register();
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

    private void listenToPubSubTopic(MessageChannel inputChannel, MessageChannel errorChannel, String subscriptionName) {
        val inputListener = new PubSubInboundChannelAdapter(pubSubTemplate, subscriptionName);
        inputListener.setOutputChannel(inputChannel);
        inputListener.setErrorChannel(errorChannel);
        inputListener.setAckMode(AckMode.MANUAL);
        inputListener.start();
    }

    private void initTopicsAndSubscriptions() {
        createTopic(getTopicName());
        createTopic(getRetryTopicName());
        createTopic(getFailedTopicName());
        createSubscription(getSubscriptionName(), getTopicName());
        createSubscription(getRetrySubscriptionName(), getRetryTopicName());
    }

    private void failedMessage(Message message) {
        val errorMessage = (ErrorMessage) message;
        val originalMessage = errorMessage.getOriginalMessage().getHeaders().get(GcpPubSubHeaders.ORIGINAL_MESSAGE, BasicAcknowledgeablePubsubMessage.class);
        val originalPayload = errorMessage.getOriginalMessage().getPayload();

        val currentRetryNumber = getRetryNumber(errorMessage.getOriginalMessage().getHeaders());
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

        originalMessage.ack();
    }

    private int getRetryNumber(MessageHeaders headers) {
        if (!headers.containsKey(RETRY_NUMBER)) {
            return 0;
        }
        return Integer.parseInt((String) headers.get(RETRY_NUMBER));
    }
}
