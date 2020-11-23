package com.reelevant.pinot.plugins.stream.pubsub;

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
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class PubSubStreamLevelConsumer implements StreamLevelConsumer {
	private final static Logger LOGGER = LoggerFactory.getLogger(PubSubStreamLevelConsumer.class);
	private final static Integer MAX_MESSAGES_PULLED_PER_BATCH = 10;

	private final StreamMessageDecoder messageDecoder;
	private PubSubStreamLevelStreamConfig pubSubStreamLevelStreamConfig;

	private List<String> ackIds = new ArrayList<>();
	private Iterator<ReceivedMessage> pubsubMessageIterator;
	private PullRequest pubsubPullRequest;
	private SubscriberStub pubsubSubscriber;
	private final String pubsubSubscriptionName;

	public PubSubStreamLevelConsumer(String clientId, String tableName, StreamConfig streamConfig, Set<String> sourceFields, String groupId) {
		pubSubStreamLevelStreamConfig = new PubSubStreamLevelStreamConfig(streamConfig, tableName, groupId);
		messageDecoder = StreamDecoderProvider.create(streamConfig, sourceFields);

		// Build Pub/Sub subscription path based on user's config
		pubsubSubscriptionName = ProjectSubscriptionName.format(
			pubSubStreamLevelStreamConfig.getProjectId(),
			pubSubStreamLevelStreamConfig.getSubscriptionId()
		);

		LOGGER.info("Pub/Sub subscription {}", pubsubSubscriptionName);
	}

	@Override
	public void start() throws Exception {
		SubscriberStubSettings subscriberStubSettings = SubscriberStubSettings.newBuilder()
			.setTransportChannelProvider(
				SubscriberStubSettings
					.defaultGrpcTransportProviderBuilder()
					.setMaxInboundMessageSize(20 * 1024 * 1024) // 20MB (maximum message size).
					.build()
			).build();

		LOGGER.info("SUBSCRIBER SETTINGS {}", subscriberStubSettings.toString());

		// Try to connect to Pub/Sub
		pubsubSubscriber = GrpcSubscriberStub.create(subscriberStubSettings);
	}

	@Override
	public GenericRow next(GenericRow destination) {
		if (pubsubMessageIterator == null || !pubsubMessageIterator.hasNext()) {
			updatePubsubMessageIterator();
		}

		if (pubsubMessageIterator.hasNext()) {
			try {
				ReceivedMessage message = pubsubMessageIterator.next();

				// Decode Pub/Sub message to Pinot's GenericRow format.
				destination = messageDecoder.decode(message.getMessage(), destination);

				// Add message id to ack list and modify the ack deadline of each
				// received message from the default 10 seconds to 30.
				// This prevents the server from redelivering the message after the default 10 seconds have passed.
				ackIds.add(message.getAckId());
				pubsubSubscriber
					.modifyAckDeadlineCallable()
					.call(ModifyAckDeadlineRequest.newBuilder()
						.setSubscription(pubsubSubscriptionName)
						.addAckIds(message.getAckId())
						.setAckDeadlineSeconds(30)
						.build());

				return destination;
			} catch (Exception exc) {
				LOGGER.warn("Caught exception while consuming Pub/Sub message", exc);
				throw exc;
			}
		}

		return null;
	}

	@Override
	public void commit() {
		AcknowledgeRequest acknowledgeRequest = AcknowledgeRequest.newBuilder()
			.setSubscription(pubsubSubscriptionName)
			.addAllAckIds(ackIds)
			.build();

		// Use acknowledgeCallable().futureCall to asynchronously perform this operation.
		pubsubSubscriber.acknowledgeCallable().call(acknowledgeRequest);
		ackIds.clear();
	}

	@Override
	public void shutdown() throws Exception {
		if (pubsubSubscriber != null) {
			pubsubSubscriber.close();
		}
	}

	private void updatePubsubMessageIterator() {
		if (pubsubPullRequest == null) {
			pubsubPullRequest = PullRequest.newBuilder()
				.setMaxMessages(MAX_MESSAGES_PULLED_PER_BATCH)
				.setSubscription(pubsubSubscriptionName)
				.build();
		}

		// Pull messages from Pub/Sub subscription and update message iterator.
		PullResponse pullResponse = pubsubSubscriber.pullCallable().call(pubsubPullRequest);
		pubsubMessageIterator = pullResponse.getReceivedMessagesList().listIterator();
	}

}
