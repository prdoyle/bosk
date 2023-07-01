package io.vena.chronicle.events;

import io.vena.chronicle.db.EventEntity;
import io.vena.chronicle.dto.EventDTO;
import java.util.List;
import org.mapstruct.Mapper;

@Mapper
interface EventMapper {
	EventDTO toDTO(EventEntity entity);
	EventEntity fromDTO(EventDTO dto);

	List<EventDTO> toDTO(List<EventEntity> entity);
	List<EventEntity> fromDTO(List<EventDTO> dto);
}
