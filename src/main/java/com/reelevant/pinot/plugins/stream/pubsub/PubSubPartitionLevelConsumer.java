/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.reelevant.pinot.plugins.stream.pubsub;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.TimeoutException;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.MessageBatch;
import org.apache.pinot.spi.stream.PartitionLevelConsumer;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;

import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PubSubPartitionLevelConsumer implements PartitionLevelConsumer {
  private static final Logger LOGGER = LoggerFactory.getLogger(PubSubPartitionLevelConsumer.class);
  private final static Integer MAX_MESSAGES_PULLED_PER_BATCH = 20;

	private PubSubPartitionLevelStreamConfig pubSubStreamLevelStreamConfig;

	private PullRequest pubsubPullRequest;
	private SubscriberStub pubsubSubscriber;
	private final String pubsubSubscriptionName;

	// Keeps information for logging
	private long currentCount = 0L;

  public PubSubPartitionLevelConsumer(String clientId, StreamConfig streamConfig, int partition) {
    pubSubStreamLevelStreamConfig = new PubSubPartitionLevelStreamConfig(streamConfig);

		// Build Pub/Sub subscription path based on user's config
		pubsubSubscriptionName = ProjectSubscriptionName.format(
			pubSubStreamLevelStreamConfig.getProjectId(),
			pubSubStreamLevelStreamConfig.getSubscriptionId()
		);

    LOGGER.info("Pub/Sub connecting (clientId {}, partition {})", clientId, partition);
    try {
      SubscriberStubSettings subscriberStubSettings = SubscriberStubSettings.newBuilder()
			.setTransportChannelProvider(
				SubscriberStubSettings
					.defaultGrpcTransportProviderBuilder()
					.setMaxInboundMessageSize(20 * 1024 * 1024) // 20MB (maximum message size).
					.build()
			).build();

		// Try to connect to Pub/Sub
    pubsubSubscriber = GrpcSubscriberStub.create(subscriberStubSettings);
    pubsubPullRequest = PullRequest.newBuilder()
				.setMaxMessages(MAX_MESSAGES_PULLED_PER_BATCH)
        .setSubscription(pubsubSubscriptionName)
        .build();
    LOGGER.info("Pubsub connected (clientId {}, partition {})", clientId, partition);
    } catch (IOException exception) {
      LOGGER.error("Failed to init pubsub", exception);
    }
  }

  @Override
  public MessageBatch fetchMessages(StreamPartitionMsgOffset startMsgOffset, StreamPartitionMsgOffset endMsgOffset,
      int timeoutMillis)
      throws TimeoutException {
    final long startOffset = ((LongMsgOffset)startMsgOffset).getOffset();
    final long endOffset = endMsgOffset == null ? Long.MAX_VALUE : ((LongMsgOffset)endMsgOffset).getOffset();
    return fetchMessages(startOffset, endOffset, timeoutMillis);
  }

  public MessageBatch fetchMessages(long startOffset, long endOffset, int timeoutMillis)
      throws TimeoutException {
    PullResponse pullResponse = pubsubSubscriber.pullCallable().call(pubsubPullRequest);
    Iterator<ReceivedMessage> iterator = pullResponse.getReceivedMessagesList().listIterator();
    PubSubMessageBatch batch = new PubSubMessageBatch(iterator, currentCount, (ArrayList<String> ackIds) -> {
      LOGGER.info("Pubsub acks (size: {})", ackIds.size());
      AcknowledgeRequest acknowledgeRequest = AcknowledgeRequest.newBuilder()
        .setSubscription(pubsubSubscriptionName)
        .addAllAckIds(ackIds)
        .build();

      // Use acknowledgeCallable().futureCall to asynchronously perform this operation.
      pubsubSubscriber.acknowledgeCallable().call(acknowledgeRequest);
    });
    currentCount += MAX_MESSAGES_PULLED_PER_BATCH;
    return batch;
  }

  @Override
  public void close()
      throws IOException {
    pubsubSubscriber.close();
  }
}
