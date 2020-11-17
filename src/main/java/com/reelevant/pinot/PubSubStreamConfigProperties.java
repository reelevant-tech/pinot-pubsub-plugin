package com.reelevant.pinot;

import com.google.common.base.Joiner;
import org.apache.pinot.spi.stream.StreamConfigProperties;

public class PubSubStreamConfigProperties {
	public static final String DOT_SEPARATOR = ".";
	public static final String STREAM_TYPE = "pubsub";

	public static String constructStreamProperty(String property) {
		return Joiner.on(DOT_SEPARATOR).join(StreamConfigProperties.STREAM_PREFIX, property);
	}

	public static class HighLevelConsumer {
		public static final String PUBSUB_PROJECT_ID = "pubsub.projectId";
		public static final String PUBSUB_SUBSCRIPTION_ID = "pubsub.subscriptionId";
		public static final String PUBSUB_TOPIC_NAME = "pubsub.topicName";
	}

	public static final String PUBSUB_CONSUMER_PROP_PREFIX = "pubsub.consumer.prop";
}
