package com.reelevant.pinot.plugins.stream.pubsub;

import org.apache.pinot.spi.stream.PartitionLevelConsumer;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.spi.stream.StreamLevelConsumer;
import org.apache.pinot.spi.stream.StreamMetadataProvider;

import java.util.Set;

public class PubSubConsumerFactory extends StreamConsumerFactory {
	@Override
	public PartitionLevelConsumer createPartitionLevelConsumer(String clientId, int partition) {
    return new PubSubPartitionLevelConsumer(clientId, _streamConfig, partition);
	}

	@Override
	public StreamLevelConsumer createStreamLevelConsumer(String clientId, String tableName, Set<String> fieldsToRead, String groupId) {
		throw new UnsupportedOperationException("Pub/Sub stream plugin does not support stream level.");
	}

	@Override
	public StreamMetadataProvider createPartitionMetadataProvider(String clientId, int partition) {
    return new PubSubMetadataProvider(clientId, _streamConfig);
	}

	@Override
	public StreamMetadataProvider createStreamMetadataProvider(String clientId) {
    return new PubSubMetadataProvider(clientId, _streamConfig);
	}
}
