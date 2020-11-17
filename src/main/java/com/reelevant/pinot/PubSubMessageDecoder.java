package com.reelevant.pinot;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractor;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

/**
 * An implementation of Stream Message Decoder to read PubsubMessage from a Pub/Sub topic.
 * We copied the same model that for Kafka plugin but added support for PubsubMessage.
 *
 * See Kafka implementation here:
 * https://github.com/apache/incubator-pinot/blob/master/pinot-plugins/pinot-stream-ingestion/pinot-kafka-base/src/main/java/org/apache/pinot/plugin/stream/kafka/KafkaJSONMessageDecoder.java
 */
public class PubSubMessageDecoder implements StreamMessageDecoder<PubsubMessage> {

	private static final Logger LOGGER = LoggerFactory.getLogger(PubSubMessageDecoder.class);
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
	private static final String DEFAULT_JSON_EXTRACTOR_CLASS = "org.apache.pinot.plugin.inputformat.json.JSONRecordExtractor";

	private RecordExtractor<Map<String, Object>> jsonRecordExtractor;

	@Override
	public void init(Map<String, String> props, Set<String> fieldsToRead, String subscriptionId) throws Exception {
		String recordExtractorClass = null;

		// Try to get custom JSON extractor class from properties
		if (props != null) {
			recordExtractorClass = props.get(RECORD_EXTRACTOR_CONFIG_KEY);
		}

		// Fallback to default JSON extractor key
		if (recordExtractorClass == null) {
			recordExtractorClass = DEFAULT_JSON_EXTRACTOR_CLASS;
		}

		jsonRecordExtractor = PluginManager.get().createInstance(recordExtractorClass);
		jsonRecordExtractor.init(fieldsToRead, null);
	}

	@Override
	public GenericRow decode(PubsubMessage payload, GenericRow destination) {
		try {
			JsonNode message = JsonUtils.bytesToJsonNode(payload.getData().toByteArray());
			Map<String, Object> from = OBJECT_MAPPER.convertValue(message, new TypeReference<Map<String, Object>>() {});
			jsonRecordExtractor.extract(from, destination);
			return destination;
		} catch (Exception exc) {
			LOGGER.error("Failed to parse bytes to JSON. Content was {}", payload.getData(), exc);
			return null;
		}
	}

	@Override
	public GenericRow decode(PubsubMessage payload, int offset, int length, GenericRow destination) {
		// No need to support offset with Pub/Sub, just call previous method.
		return decode(payload, destination);
	}
}
