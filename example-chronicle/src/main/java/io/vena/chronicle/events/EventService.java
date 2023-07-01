package io.vena.chronicle.events;

import io.vena.chronicle.dto.EventDTO;
import io.vena.chronicle.dto.MetadataDTO;
import io.vena.chronicle.dto.SenderDTO;
import java.time.Instant;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Configuration
public class EventService {
	private final EventRepository repository;
	private final EventMapper eventMapper;

	public void ingest(SenderDTO sender, List<EventDTO> records) {
		MetadataDTO meta = new MetadataDTO(
			Instant.now().toEpochMilli(),
			sender);
		repository.saveAll(records.stream()
			.map(eventDTO -> eventDTO.withMeta(meta))
			.map(eventMapper::fromDTO)
			.toList());
	}

}
