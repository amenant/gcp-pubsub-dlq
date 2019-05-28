# gcp-pubsub-dlq
Sample code simulating retry and DLQ for GCP Pub Sub

[Google Cloud Pub/Sub](https://cloud.google.com/pubsub/) doesn't come with a retry or dead-letter mecanism.
This small code gives an alternative to simulate retry and dead letter topics without impacting your code.
It's using [Spring Integration](https://spring.io/projects/spring-integration) and [Spring Cloud GCP](https://spring.io/projects/spring-cloud-gcp).
Spring Integration is not mandatory but could help building more complex workflows.

## How to use it?
For the version with Spring Integration, extend `AbstractPubSubListenerWithRetry` and override a few methods.
See example in [PubSubTopicListener](src/main/java/com/demo/configuration/PubSubTopicListener.java)

For the version without Spring Integration, extend `AbstractVanillaPubSubListenerWithRetry` and override methods.
See example in [PubSubVanillaTopicListener](src/main/java/com/demo/configuration/PubSubVanillaTopicListener.java)

Start the [GCP Pub Sub emulator](https://cloud.google.com/pubsub/docs/emulator) to test it.

## How to test it?
There's an endpoint pushing messages to the main topic.

Try any message and it should be displayed in the standard output (console):

`curl http://localhost:8080/send?message=hello`

`curl http://localhost:8080/send-vanilla?message=hello`

If you try a message starting with 'error', the message will throw a `RuntimeException`, triggering retries until discard.

If you want to check the dead-letter-queue:
`curl http://localhost:8080/read-dlq`