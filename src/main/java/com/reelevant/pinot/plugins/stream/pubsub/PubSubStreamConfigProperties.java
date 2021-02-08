package com.reelevant.pinot.plugins.stream.pubsub;

import com.google.common.base.Joiner;
import org.apache.pinot.spi.stream.StreamConfigProperties;

public class PubSubStreamConfigProperties {
  public static final String DOT_SEPARATOR = ".";
  public static final String PUBSUB_CONSUMER_PROP_PREFIX = "pubsub.consumer.prop";
  public static final String STREAM_TYPE = "pubsub";

  public static String constructStreamProperty(String property) {
    return Joiner.on(DOT_SEPARATOR).join(StreamConfigProperties.STREAM_PREFIX, property);
  }

  public static class HighLevelConsumer {
    public static final String PUBSUB_PROJECT_ID = "pubsub.project.id";
    public static final String PUBSUB_SUBSCRIPTION_ID = "pubsub.subscription.id";
  }

}
