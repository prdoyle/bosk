package io.vena.chronicle.events;

import com.mongodb.lang.NonNull;
import io.vena.chronicle.db.EventEntity;
import io.vena.chronicle.db.EventVariantEntity;
import java.util.List;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public interface EventRepository extends MongoRepository<EventEntity, String> {
	@Override @Transactional @NonNull <S extends EventEntity>
	List<S> saveAll(@NonNull Iterable<S> entities);
}
