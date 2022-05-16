package org.vena.bosk.drivers.dynamo;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.utility.DockerImageName;


public class DynamoContainerHelpers {
	static GenericContainer<?> dynamoContainer(Network network) {
		return new GenericContainer<>("amazon/dynamodb-local")
			.withNetwork(network)
			.withExposedPorts(8000);
	}

	static ToxiproxyContainer toxiproxyContainer(Network network) {
		return new ToxiproxyContainer(
			DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.2.0").asCompatibleSubstituteFor("shopify/toxiproxy"))
			.withNetwork(network);
	}

}
