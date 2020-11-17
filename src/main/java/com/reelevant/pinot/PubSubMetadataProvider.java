package com.reelevant.pinot;

import org.apache.pinot.spi.stream.OffsetCriteria;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nonnull;

public class PubSubMetadataProvider implements StreamMetadataProvider {

	public PubSubMetadataProvider(String clientId, StreamConfig streamConfig) {
		// Should handle low level connection
		// but are not supported for Google Pub/Sub.
		// It is still required by Pinot.
	}

	@Override
	public int fetchPartitionCount(long l) {
		return 0;
	}

	@Override
	public long fetchPartitionOffset(@Nonnull OffsetCriteria offsetCriteria, long l) throws TimeoutException {
		throw new UnsupportedOperationException("Usage of this method is not supported with Pub/Sub");
	}

	@Override
	public StreamPartitionMsgOffset fetchStreamPartitionOffset(@Nonnull OffsetCriteria offsetCriteria, long timeoutMillis) throws TimeoutException {
		throw new UnsupportedOperationException("Usage of this method is not supported with Pub/Sub");
	}

	@Override
	public void close() throws IOException {

	}

}
