package io.vena.bosk.drivers.mongo.modal;

import com.mongodb.client.FindIterable;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;

/**
 * An abstraction of a {@link com.mongodb.client.MongoCollection MongoCollection}
 * suitable for storing bosk state.
 */
public interface BoskCollection {
	FindIterable<Document> find(Bson filter);
	UpdateResult updateOne(Bson filter, Bson update, UpdateOptions updateOptions);
	UpdateResult replaceOne(Bson filter, Document replacement);
}
