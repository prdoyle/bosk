package io.vena.bosk.immutable;

import io.vena.bosk.immutable.TreeNodeTest.DistinctValue;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static io.vena.bosk.immutable.TreeNodeTest.assertBalanced;
import static io.vena.bosk.immutable.TreeNodeTest.assertEntriesEqual;
import static io.vena.bosk.immutable.TreeNodeTest.assertEquivalentToTreeMap;
import static io.vena.bosk.immutable.TreeNodeTest.entryList;
import static org.junit.jupiter.api.Assertions.fail;

public class TreeNodeRandomTest {
	@ParameterizedTest
	@MethodSource("randomSeeds")
	void randomListFewCollisions_equivalent(long seed) {
		assertEquivalentToTreeMap(
			entryList(new Random(seed).ints(1000, 0, 1_000_000)),
			String::compareTo
		);
	}

	@ParameterizedTest
	@MethodSource("randomSeeds")
	void randomListWithManyCollisions_equivalent(long seed) {
		assertEquivalentToTreeMap(
			entryList(new Random(seed).ints(1000, 0, 20)),
			String::compareTo
		);
	}

	@ParameterizedTest
	@MethodSource("randomSeeds")
	void randomOperations_equivalent(long seed) {
		Random random = new Random(seed);
		TreeNode<String, DistinctValue> treeNode = TreeNode.empty();
		TreeMap<String, DistinctValue> treeMap = new TreeMap<>();
		List<String> allKeys = new ArrayList<>();
		for (int i = random.nextInt(10000); i > 0; i--) {
			if (random.nextDouble(1.0) > 0.8) {
				// Add a random item
				int id = random.nextInt(100);
				String key = "key_" + id;
				DistinctValue value = new DistinctValue(Integer.toString(id));
				treeNode = treeNode.with(key, value, String::compareTo);
				treeMap.put(key, value);
				allKeys.add(key);
			}
			if (!allKeys.isEmpty() && random.nextDouble(1.0) > 0.1) {
				// Choose a random key and delete it
				String key = allKeys.get(random.nextInt(allKeys.size()));
				treeNode = treeNode.without(key, String::compareTo);
				treeMap.remove(key);
			}
			if (random.nextDouble(1.0) > 0.1) {
				// Invent a random item and delete it
				int id = random.nextInt(100);
				String key = "key_" + id;
				treeNode = treeNode.without(key, String::compareTo);
				treeMap.remove(key);
			}
			assertEntriesEqual(treeNode, treeMap);
			assertBalanced(treeNode);
		}

	}

	public static Stream<Arguments> randomSeeds() {
		return new Random(123)
			.longs(100)
			.mapToObj(Arguments::of);
	}

}
