package io.vena.chronicle.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.springframework.lang.Nullable;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

@JsonInclude(NON_NULL)
public record EventVariantDTO(
	@Nullable StartRecordDTO start,
	@Nullable StopRecordDTO stop
){
	public static EventVariantDTO of(StartRecordDTO start) {
		return new EventVariantDTO(start, null);
	}

	public static EventVariantDTO of(StopRecordDTO stop) {
		return new EventVariantDTO(null, stop);
	}

}
