package com.reelevant.pinot;

import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.*;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamDecoderProvider;
import org.apache.pinot.spi.stream.StreamLevelConsumer;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;


public class PubSubStreamLevelConsumer implements StreamLevelConsumer {
	private final Logger LOGGER;
	private static final Integer MAXIMUM_NUM_OF_EVENTS_BEFORE_LOGGING = 10000;
	private static final Integer MAXIMUM_MS_BEFORE_LOGGING = 60000;

	private long CURRENT_COUNT = 0;
	private long PREVIOUS_COUNT = 0;
	private long LAST_LOG_TIME = 0;

	// Maximum number of events to be pulled from Pub/Sub
	// between each batch. Will populate Pub/Sub iterator used in next method.
	private final static Integer MAX_NUM_EVENTS_PER_BATCH = 10;

	private final StreamMessageDecoder messageDecoder;

	// Component to pull events from Pub/Sub and ack them.
	private List<String> ackIds = new ArrayList<>();
	private ListIterator<ReceivedMessage> pubsubIterator;
	private PullRequest pullRequest;
	private SubscriberStub subscriber;
	private final String subscriptionName;

	public PubSubStreamLevelConsumer(String projectId, String subscriptionId, StreamConfig streamConfig, Set<String> fieldsToRead) {
		messageDecoder = StreamDecoderProvider.create(streamConfig, fieldsToRead);
		subscriptionName = ProjectSubscriptionName.format(projectId, subscriptionId);

		LOGGER = LoggerFactory.getLogger(PubSubStreamLevelConsumer.class.getName());
	}

	@Override
	public void start() throws Exception {
		// 20MB (maximum message size).
		SubscriberStubSettings subscriberSettings = SubscriberStubSettings.newBuilder()
				.setTransportChannelProvider(
						SubscriberStubSettings
								.defaultGrpcTransportProviderBuilder()
								.setMaxInboundMessageSize(20 * 1024 * 1024) // 20MB (maximum message size).
								.build()
				).build();
		subscriber = GrpcSubscriberStub.create(subscriberSettings);
		pullRequest = PullRequest.newBuilder()
				.setMaxMessages(MAX_NUM_EVENTS_PER_BATCH)
				.setSubscription(subscriptionName)
				.build();
	}

	private void updatePubsubIterator() {
		PullResponse pullResponse = subscriber.pullCallable().call(pullRequest);
		pubsubIterator = pullResponse.getReceivedMessagesList().listIterator();
	}

	@Override
	public GenericRow next(GenericRow destination) {
		if (pubsubIterator == null || !pubsubIterator.hasNext()) {
			updatePubsubIterator();
		}

		if (pubsubIterator.hasNext()) {
			try {
				final ReceivedMessage message = pubsubIterator.next();
				destination = messageDecoder.decode(message, destination);
				ackIds.add(message.getAckId());
				// Check if we need to log some information to the user
				CURRENT_COUNT++;
				final long now = System.currentTimeMillis();
				if (now - LAST_LOG_TIME > MAXIMUM_MS_BEFORE_LOGGING || CURRENT_COUNT - PREVIOUS_COUNT >= MAXIMUM_NUM_OF_EVENTS_BEFORE_LOGGING) {
					if (PREVIOUS_COUNT == 0) {
						LOGGER.info("Consumed {} events from Pub/Sub subscription {}", CURRENT_COUNT, this.subscriptionName);
					} else {
						LOGGER.info("Consumed {} events from kafka stream {} (rate:{}/s)", CURRENT_COUNT - PREVIOUS_COUNT,
								this.subscriptionName, (float) (CURRENT_COUNT - PREVIOUS_COUNT) * 1000 / (now - LAST_LOG_TIME));
					}
					PREVIOUS_COUNT = CURRENT_COUNT;
					LAST_LOG_TIME = now;
				}
			} catch (Exception exc) {
				LOGGER.warn("Caught exception while consuming events", exc);
				throw exc;
			}

			return destination;
		}

		return null;
	}

	@Override
	public void commit() {
		subscriber.acknowledgeCallable().call(
				AcknowledgeRequest.newBuilder()
						.setSubscription(subscriptionName)
						.addAllAckIds(ackIds)
						.build()
		);
		ackIds.clear();
	}

	@Override
	public void shutdown() throws Exception {
		if (subscriber != null) {
			subscriber.close();
		}
	}
}
