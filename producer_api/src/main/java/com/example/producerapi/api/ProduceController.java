package com.example.producerapi.api;

import com.example.producerapi.config.AppKafkaProperties;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@RestController
public class ProduceController {

	private final KafkaTemplate<String, String> kafkaTemplate;
	@PostMapping("/send")
	public SendResponse send(@RequestBody SendRequest req) {
		String type = (req.type() == null || req.type().isBlank()) ? "UI_EVENT" : req.type();
		String message = (req.message() == null) ? "" : req.message();

		// JSON 문자열로 보내기 (실무에선 스키마/직렬화로 발전)
		String payload = toJson(type, message);

		kafkaTemplate.send(props.topic(), type, payload);

		return new SendResponse(true, props.topic(), payload);
	}

	private final AppKafkaProperties props;

	public ProduceController(KafkaTemplate<String, String> kafkaTemplate, AppKafkaProperties props) {
		this.kafkaTemplate = kafkaTemplate;
		this.props = props;
	}

	private String toJson(String type, String message) {
		long ts = Instant.now().toEpochMilli();
		String messageId = UUID.randomUUID().toString(); // UUID 생성
		String safeMessage = message.replace("\\", "\\\\").replace("\"", "\\\"");
		return "{\"messageId\":\"" + messageId + "\",\"type\":\"" + type + "\",\"message\":\"" + safeMessage + "\",\"ts\":" + ts + "}";
	}

	@GetMapping("/health")
	public Map<String, Object> health() {
		return Map.of("ok", true, "topic", props.topic());
	}
}