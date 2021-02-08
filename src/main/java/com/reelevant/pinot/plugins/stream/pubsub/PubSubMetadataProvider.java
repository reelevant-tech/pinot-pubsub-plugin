package com.reelevant.pinot.plugins.stream.pubsub;

import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.OffsetCriteria;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;

import java.io.IOException;
import javax.annotation.Nonnull;

public class PubSubMetadataProvider implements StreamMetadataProvider {
	public PubSubMetadataProvider(String clientId, StreamConfig streamConfig) {}

	@Override
	public int fetchPartitionCount(long timeoutMillis) {
    // we always have one partition
		return 1;
	}

	@Override
	public long fetchPartitionOffset(@Nonnull OffsetCriteria offsetCriteria, long l) throws UnsupportedOperationException {
		throw new UnsupportedOperationException("Usage of this method is not supported with Pub/Sub");
	}

	@Override
	public StreamPartitionMsgOffset fetchStreamPartitionOffset(@Nonnull OffsetCriteria offsetCriteria, long timeoutMillis) {
    // we always fake that we start from offset 0
    return new LongMsgOffset(0);
	}

	@Override
	public void close() throws IOException {}
}
