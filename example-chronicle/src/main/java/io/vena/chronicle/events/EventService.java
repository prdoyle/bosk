package io.vena.chronicle.events;

import io.vena.chronicle.dto.EventDTO;
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

	public void ingest(List<EventDTO> records) {
		repository.saveAll(records.stream()
			.map(EventDTO::ingestedNow)
			.map(eventMapper::fromDTO)
			.toList());
	}
}
