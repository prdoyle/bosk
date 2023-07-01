package io.vena.chronicle.events;

import io.vena.chronicle.dto.EventDTO;
import io.vena.chronicle.dto.IngestRequestDTO;
import io.vena.chronicle.dto.SenderDTO;
import jakarta.servlet.http.HttpServletRequest;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("events")
@RequiredArgsConstructor
public class EventEndpoints {
	private final EventService eventService;
	private final EventRepository eventRepository;
	private final EventMapper eventMapper;

	@PostMapping("ingest")
	public void ingest(
		HttpServletRequest req,
		@RequestBody IngestRequestDTO body
	) {
		eventService.ingest(
			new SenderDTO(req.getRemoteAddr()),
			body.items());
	}

	@GetMapping("list")
	public List<EventDTO> list() {
		return eventRepository.findAll().stream()
			.map(eventMapper::toDTO)
			.toList();
	}
}
