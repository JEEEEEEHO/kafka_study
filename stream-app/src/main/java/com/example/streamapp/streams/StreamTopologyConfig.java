package com.example.streamapp.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
// 추가 import들
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import java.time.Duration;

@Configuration
@EnableKafkaStreams
public class StreamTopologyConfig {

	private static final String INPUT_TOPIC = "demo.ui-events";

	private static final String CLICK_TOPIC = "demo.click-events";   // A
	private static final String VIEW_TOPIC  = "demo.view-events";    // B
	private static final String UNKNOWN_TOPIC = "demo.unknown-events";

	private final ObjectMapper objectMapper = new ObjectMapper();

	@Bean
	public KStream<String, String> streamPipeline(StreamsBuilder builder) {
		KStream<String, String> source =
			builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));

		// 1) value -> type 추출해서 key를 type으로 맞춰줌(집계/디버깅에 유리)
		KStream<String, String> typed = source
			.filter((k, v) -> v != null && !v.isBlank())
			.selectKey((k, v) -> extractTypeOr(v, "PARSE_ERROR"))
			.peek((k, v) -> System.out.println("[IN ] type=" + k + " value=" + v));

		// 2) branch: 조건에 따라 스트림을 여러 갈래로 분기
		KStream<String, String>[] branches = typed.branch(
			(key, value) -> "CLICK".equalsIgnoreCase(key),
			(key, value) -> "VIEW".equalsIgnoreCase(key),
			(key, value) -> true // 나머지는 전부 여기로
		);

		KStream<String, String> clickStream = branches[0];
		KStream<String, String> viewStream  = branches[1];
		KStream<String, String> unknownStream = branches[2];

		// 3) 각 토픽으로 전송
		clickStream
			.mapValues(v -> "CLICK_STREAM: " + v)
			.peek((k, v) -> System.out.println("[OUT-CLICK] " + k + " " + v))
			.to(CLICK_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

		viewStream
			.mapValues(v -> "VIEW_STREAM: " + v)
			.peek((k, v) -> System.out.println("[OUT-VIEW] " + k + " " + v))
			.to(VIEW_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

		unknownStream
			.mapValues(v -> "UNKNOWN_STREAM: " + v)
			.peek((k, v) -> System.out.println("[OUT-UNKNOWN] " + k + " " + v))
			.to(UNKNOWN_TOPIC, Produced.with(Serdes.String(), Serdes.String()));


		return source;
	}

	private String extractTypeOr(String json, String fallback) {
		try {
			JsonNode node = objectMapper.readTree(json);
			JsonNode typeNode = node.get("type");
			if (typeNode == null || typeNode.isNull()) return "UNKNOWN";
			String type = typeNode.asText();
			return (type == null || type.isBlank()) ? "UNKNOWN" : type;
		} catch (Exception e) {
			return fallback; // PARSE_ERROR
		}
	}
}