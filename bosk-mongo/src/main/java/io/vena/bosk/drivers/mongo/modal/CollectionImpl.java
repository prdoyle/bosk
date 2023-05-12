package io.vena.bosk.drivers.mongo.modal;

import com.mongodb.client.MongoCollection;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Delegate;
import org.bson.Document;

@RequiredArgsConstructor
public class CollectionImpl implements BoskCollection {
	@Delegate(types={BoskCollection.class})
	private final MongoCollection<Document> collection;
}
