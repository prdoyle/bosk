package io.vena.bosk.drivers.mongo;

import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ChangeStreamExperiments {
	static MongoService mongoService;
	static MongoDatabase database;

	@BeforeAll
	static void setupMongoService() {
		mongoService = new MongoService();
		database = mongoService.client().getDatabase(ChangeStreamExperiments.class.getSimpleName());
	}

	@Test
	void newChangeStreamHasNoResumeToken() {
		database.createCollection("collection1");
		MongoCollection<Document> collection1 = database
			.getCollection("collection1");
		collection1.insertOne(new Document().append("_id", "doc1"));

		try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> changeStream =
				 collection1.watch().cursor()
		) {
			ChangeStreamDocument<Document> next = changeStream.tryNext();
			assertNull(next, "No events in the change stream yet");
//			changeStream.hasNext();
			System.out.println("Token 1: " + changeStream.getResumeToken());
			assertNotNull(changeStream.getResumeToken(), "tryNext established a resume token");
			collection1.insertOne(new Document().append("_id", "doc2"));
			changeStream.next();
			System.out.println("Token 2: " + changeStream.getResumeToken());
			assertNotNull(changeStream.getResumeToken());
		}
	}
}
