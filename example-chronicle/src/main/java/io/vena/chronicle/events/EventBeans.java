package io.vena.chronicle.events;

import org.mapstruct.factory.Mappers;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
class EventBeans {
	@Bean
	EventMapper eventMapper() {
		return Mappers.getMapper(EventMapper.class);
	}
}
