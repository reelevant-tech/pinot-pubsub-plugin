package com.reelevant.pinot;

import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PubSubStreamLevelStreamConfig {
	private static final Map<String, String> defaultProps;
	private final String projectId;
	private final String subscriptionId;
	private final String topicName;
	private final Map<String, String> pubSubConsumerProperties;

	//public PubSubStreamLevelStreamConfig(StreamConfig streamConfig, String projectId, String subscriptionId, String topicName) {
	public PubSubStreamLevelStreamConfig(StreamConfig streamConfig, String tableName, String groupId) {
		Map<String, String> streamConfigMap = streamConfig.getStreamConfigsMap();

		this.pubSubConsumerProperties = new HashMap<>();
		String pubSubConsumerPropertyPrefix = PubSubStreamConfigProperties.constructStreamProperty(PubSubStreamConfigProperties.PUBSUB_CONSUMER_PROP_PREFIX);
		for (String key: streamConfigMap.keySet()) {
			if (key.startsWith(pubSubConsumerPropertyPrefix)) {
				pubSubConsumerProperties
					.put(StreamConfigProperties.getPropertySuffix(key, pubSubConsumerPropertyPrefix), streamConfigMap.get(key));
			}
		}

		this.projectId = this.pubSubConsumerProperties.get("projectId");
		this.subscriptionId = this.pubSubConsumerProperties.get("subscriptionId");
		this.topicName = this.pubSubConsumerProperties.get("topicName");
	}

	public Properties getPubSubConsumerProperties() {
		Properties props = new Properties();
		for (String key: defaultProps.keySet()) {
			props.put(key, defaultProps.get(key));
		}

		for (String key: pubSubConsumerProperties.keySet()) {
			props.put(key, pubSubConsumerProperties.get(key));
		}

		return props;
	}

	public String getProjectId() {
		return this.projectId;
	}

	public String getSubscriptionId() {
		return this.subscriptionId;
	}

	public String getTopicName() {
		return this.topicName;
	}

	static {
		// Default PubSub properties (if any) should be defined here.
		defaultProps = new HashMap<>();
		defaultProps.put("stream.pubsub.consumer.type", "HighLevel");
		defaultProps.put("stream.pubsub.consumer.factory.class.name", "com.reelevant.pinot.PubSubConsumerFactory");
	}
}
