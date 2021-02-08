package com.reelevant.pinot.plugins.stream.pubsub;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractor;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

public class PubSubMessageDecoder implements StreamMessageDecoder<byte[]> {
	private static final String JSON_RECORD_EXTRACTOR_CLASS = "org.apache.pinot.plugin.inputformat.json.JSONRecordExtractor";
	private static final Logger LOGGER = LoggerFactory.getLogger(PubSubMessageDecoder.class);
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private RecordExtractor<Map<String, Object>> jsonRecordExtractor;

	@Override
	public void init(Map<String, String> props, Set<String> fieldsToRead, String topicId) throws Exception {
		String recordExtractorClass = null;

		if (props != null) {
			recordExtractorClass = props.get(RECORD_EXTRACTOR_CONFIG_KEY);
		}

		if (recordExtractorClass == null) {
			recordExtractorClass = JSON_RECORD_EXTRACTOR_CLASS;
		}

		jsonRecordExtractor = PluginManager.get().createInstance(recordExtractorClass);
		jsonRecordExtractor.init(fieldsToRead, null);
	}

	@Override
	public GenericRow decode(byte[] payload, GenericRow destination) {
		try {
			JsonNode message = JsonUtils.bytesToJsonNode(payload);
      Map<String, Object> from = OBJECT_MAPPER.convertValue(message, new TypeReference<Map<String, Object>>() {
      });
			jsonRecordExtractor.extract(from, destination);
			return destination;
		} catch (Exception exc) {
			LOGGER.error("Failed to parse bytes to JSON. Content was {}", payload.toString(), exc);
			return null;
		}
	}

	@Override
	public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
    return decode(Arrays.copyOfRange(payload, offset, offset + length), destination);
	}
}