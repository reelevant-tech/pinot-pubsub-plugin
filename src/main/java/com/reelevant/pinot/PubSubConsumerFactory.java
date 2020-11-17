package com.reelevant.pinot;

import org.apache.pinot.spi.stream.PartitionLevelConsumer;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.spi.stream.StreamLevelConsumer;
import org.apache.pinot.spi.stream.StreamMetadataProvider;

import java.util.Set;

public class PubSubConsumerFactory extends StreamConsumerFactory {

	@Override
	public StreamMetadataProvider createPartitionMetadataProvider(String clientId, int partition) {
		throw new UnsupportedOperationException("Low level stream type is not supported by Pub/Sub plugin.");
	}

	@Override
	public StreamMetadataProvider createStreamMetadataProvider(String clientId) {
		return new PubSubMetadataProvider(clientId, _streamConfig);
	}

	@Override
	public PartitionLevelConsumer createPartitionLevelConsumer(String clientId, int partition) {
		throw new UnsupportedOperationException("Low level stream type is not supported by Pub/Sub plugin.");
	}

	@Override
	public StreamLevelConsumer createStreamLevelConsumer(String clientId, String tableName, Set<String> fieldsToRead, String groupId) {
		return new PubSubStreamLevelConsumer(clientId, tableName, _streamConfig, fieldsToRead, groupId);
	}

}
