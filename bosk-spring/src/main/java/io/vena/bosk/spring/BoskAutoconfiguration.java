package io.vena.bosk.spring;

import io.vena.bosk.Bosk;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class BoskAutoconfiguration {

	@ConditionalOnClass(Bosk.class)
	ReadContextFilter readContextFilter(List<Bosk<?>> bosks) {
		bosks.forEach();
		ReadContextFilter
		return new ReadContextFilter(bosks);
	}


}
