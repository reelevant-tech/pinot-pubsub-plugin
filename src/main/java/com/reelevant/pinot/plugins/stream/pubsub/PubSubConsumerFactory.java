package com.reelevant.pinot.plugins.stream.pubsub;

import org.apache.pinot.spi.stream.PartitionLevelConsumer;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.spi.stream.StreamLevelConsumer;
import org.apache.pinot.spi.stream.StreamMetadataProvider;

import java.util.Set;

public class PubSubConsumerFactory extends StreamConsumerFactory {
	@Override
	public PartitionLevelConsumer createPartitionLevelConsumer(String s, int i) {
		throw new UnsupportedOperationException("Pub/Sub stream plugin does not support partition level.");
	}

	@Override
	public StreamLevelConsumer createStreamLevelConsumer(String clientId, String tableName, Set<String> fieldsToRead, String groupId) {
		return new PubSubStreamLevelConsumer(clientId, tableName, _streamConfig, fieldsToRead, groupId);
	}

	@Override
	public StreamMetadataProvider createPartitionMetadataProvider(String s, int i) {
		throw new UnsupportedOperationException("Pub/Sub stream plugin does not support partition level.");
	}

	@Override
	public StreamMetadataProvider createStreamMetadataProvider(String s) {
		return null;
	}
}
