package io.vena.chronicle.serviceability;

import io.vena.chronicle.dto.EventDTO;
import io.vena.chronicle.dto.EventListDTO;
import io.vena.chronicle.dto.EventVariantDTO;
import io.vena.chronicle.dto.MetadataDTO;
import io.vena.chronicle.dto.SpanDataDTO;
import io.vena.chronicle.dto.StartRecordDTO;
import io.vena.chronicle.dto.StopRecordDTO;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import org.bson.json.JsonObject;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import static java.lang.System.identityHashCode;
import static java.lang.Thread.currentThread;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

@RestController
@RequestMapping("dev")
public class DevEndpoints {

	@GetMapping("now")
	EventListDTO nowSpan() {
		return new EventListDTO(asList(
			new EventDTO(
				new MetadataDTO(now()),
				EventVariantDTO.of(new StartRecordDTO(
					"span1",
					new SpanDataDTO("now", "trace1", null),
					Map.of(
						"startTime", now(),
						"service", "chronicle",
						"host", "trebek",
						"thread", identityHashCode(currentThread())
					)))),
			new EventDTO(
				new MetadataDTO(now()),
				EventVariantDTO.of(new StopRecordDTO(
					"span1",
					Map.of(
						"numWidgets", 123,
						"numGrommets", 456,
						"stopTime", now()
					))))
		));
	}

	private static long now() {
		return Instant.now().toEpochMilli();
	}
}
