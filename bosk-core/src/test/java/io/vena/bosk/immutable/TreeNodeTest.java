package io.vena.bosk.immutable;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.shaded.org.apache.commons.lang3.builder.ToStringExclude;

import static io.vena.bosk.immutable.OccupiedNode.BALANCE_RATIO;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TreeNodeTest {

	@Test
	void emptyTree_sizeIsZero() {
		assertEquals(0, TreeNode.empty().size());
	}

	@Test
	void emptyWithOne_expectedStructure() {
		TreeNode<String, String> expected =
			new OccupiedNode<>("key", "value",
				TreeNode.empty(),
				TreeNode.empty());
		TreeNode<String, String> actual = empty()
			.with("key", "value", String::compareTo);
		assertEquals(expected, actual);
	}

	@Test
	void ascendingOrder_equivalent() {
		assertEquivalentToTreeMap(
			entryList(IntStream.rangeClosed(1, 3)),
			String::compareTo);
	}

	@Test
	void descendingOrder_equivalent() {
		assertEquivalentToTreeMap(
			entryList(IntStream.rangeClosed(3, 1)),
			String::compareTo);
	}

	@Test
	void mixedOrder_equivalent() {
		assertEquivalentToTreeMap(
			// This order was observed to trigger a bug
			entryList(IntStream.of(850536, 447530, 534792, 231632, 718173, 704060)),
			String::compareTo);
	}

	@Test
	void deleteSingleton_isEmpty() {
		assertEquivalentToTreeMap(
			entryList(IntStream.of(1)),
			entryList(IntStream.of(1)),
			emptyList(),
			String::compareTo
		);
	}

	@Test
	void deleteLeftDoubletonRoot_equivalent() {
		assertEquivalentToTreeMap(
			// Set up a left-leaning two-node tree
			entryList(IntStream.of(2, 1)),
			// Delete the root
			entryList(IntStream.of(2)),
			emptyList(),
			String::compareTo
		);
	}

	@Test
	void deleteLeftDoubletonLeaf_equivalent() {
		assertEquivalentToTreeMap(
			// Set up a left-leaning two-node tree
			entryList(IntStream.of(2, 1)),
			// Delete the leaf
			entryList(IntStream.of(1)),
			emptyList(),
			String::compareTo
		);
	}

	@Test
	void deleteRightDoubletonRoot_equivalent() {
		assertEquivalentToTreeMap(
			// Set up a right-leaning two-node tree
			entryList(IntStream.of(1, 2)),
			// Delete the root
			entryList(IntStream.of(1)),
			emptyList(),
			String::compareTo
		);
	}

	@Test
	void deleteRightDoubletonLeaf_equivalent() {
		assertEquivalentToTreeMap(
			// Set up a right-leaning two-node tree
			entryList(IntStream.of(1, 2)),
			// Delete the root
			entryList(IntStream.of(2)),
			emptyList(),
			String::compareTo
		);
	}

	@Test
	void deleteTripletonRoot_equivalent() {
		assertEquivalentToTreeMap(
			entryList(IntStream.of(2, 1, 3)),
			// Delete the root
			entryList(IntStream.of(1)),
			emptyList(),
			String::compareTo
		);
	}

	@Test
	void deleteTripletonLeftLeaf_equivalent() {
		assertEquivalentToTreeMap(
			entryList(IntStream.of(2, 1, 3)),
			// Delete the root
			entryList(IntStream.of(2)),
			emptyList(),
			String::compareTo
		);
	}

	@Test
	void deleteTripletonRightLeaf_equivalent() {
		assertEquivalentToTreeMap(
			entryList(IntStream.of(2, 1, 3)),
			// Delete the root
			entryList(IntStream.of(3)),
			emptyList(),
			String::compareTo
		);
	}

	@Test
	void singleLeftRotation_equivalent() {
		assertEquivalentToTreeMap(
			// Set up a right-leaning tree with a right-leaning subtree
			entryList(IntStream.of(
				20, // root
				10, // left
				70, 60, 80, 79, 81 // right - assuming BALANCE_RATIO is 5
			)),
			emptyList(),
			// Rotate!
			entryList(IntStream.of(50)),
			String::compareTo);
	}

	@Test
	void doubleLeftRotation_equivalent() {
		assertEquivalentToTreeMap(
			// Set up a right-leaning tree with a left-leaning subtree
			entryList(IntStream.of(
				20, // root
				10, // left
				70, 60, 80, 59, 61 // right - assuming BALANCE_RATIO is 5
			)),
			emptyList(),
			// Rotate!
			entryList(IntStream.of(50)),
			String::compareTo);
	}

	@Test
	void singleRightRotation_equivalent() {
		assertEquivalentToTreeMap(
			// Set up a left-leaning tree with a left-leaning subtree
			entryList(IntStream.of(
				80, // root
				90, // right
				30, 40, 20, 21, 19 // left - assuming BALANCE_RATIO is 5
			)),
			emptyList(),
			// Rotate!
			entryList(IntStream.of(50)),
			String::compareTo);
	}

	@Test
	void doubleRightRotation_equivalent() {
		assertEquivalentToTreeMap(
			// Set up a left-leaning tree with a right-leaning subtree
			entryList(IntStream.of(
				80, // root
				90, // left
				30, 40, 20, 41, 39 // right - assuming BALANCE_RATIO is 5
			)),
			emptyList(),
			// Rotate!
			entryList(IntStream.of(50)),
			String::compareTo);
	}

	@ParameterizedTest
	@MethodSource("randomSeeds")
	void randomList_equivalent(long seed) {
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

	public static Stream<Arguments> randomSeeds() {
		return new Random(123)
			.longs(20)
			.mapToObj(Arguments::of);
	}

	@NotNull
	private static List<Map.Entry<String, DistinctValue>> entryList(IntStream numbers) {
		return numbers
			.mapToObj(i -> String.format("%012d", i))
			.map(s -> new SimpleEntry<>("key" + s, new DistinctValue(s)))
			.collect(toList());
	}

	/**
	 * These inherit {@link Object#equals} and {@link Object#hashCode()}
	 * so that each instance will compare distinct, even if created using
	 * the same String.
	 */
	@RequiredArgsConstructor
	static final class DistinctValue {
		final String description;

		@Override
		public String toString() {
			return String.format("%s(%s)", System.identityHashCode(this), description);
		}

	}

	private static TreeNode<String, String> empty() {
		return TreeNode.empty();
	}

	private static <K,V> void assertEquivalentToTreeMap(
		Iterable<Map.Entry<K, V>> entriesToAdd,
		Comparator<K> comparator
	) {
		assertEquivalentToTreeMap(entriesToAdd, emptyList(), emptyList(), comparator);
	}

	private static <K,V> void assertEquivalentToTreeMap(
		Iterable<Map.Entry<K, V>> entriesToAdd,
		Iterable<Map.Entry<K, V>> entriesToRemove,
		Iterable<Map.Entry<K, V>> moreEntriesToAdd,
		Comparator<K> comparator
	) {
		TreeNode<K,V> treeNode = TreeNode.<K,V>empty();
		for (Map.Entry<K, V> entry: entriesToAdd) {
			treeNode = treeNode.with(entry.getKey(), entry.getValue(), comparator);
		}
		for (Map.Entry<K, V> entry: entriesToRemove) {
			treeNode = treeNode.without(entry.getKey(), comparator);
		}
		for (Map.Entry<K, V> entry: moreEntriesToAdd) {
			treeNode = treeNode.with(entry.getKey(), entry.getValue(), comparator);
		}

		TreeMap<K,V> treeMap = new TreeMap<>(comparator);
		for (Map.Entry<K, V> entry: entriesToAdd) {
			treeMap.put(entry.getKey(), entry.getValue());
		}
		for (Map.Entry<K, V> entry: entriesToRemove) {
			treeMap.remove(entry.getKey());
		}
		for (Map.Entry<K, V> entry: moreEntriesToAdd) {
			treeMap.put(entry.getKey(), entry.getValue());
		}

		List<Map.Entry<K,V>> actualEntries = new ArrayList<>();
		treeNode.entryIterator().forEachRemaining(actualEntries::add);

		List<Map.Entry<K,V>> expectedEntries = new ArrayList<>();
		treeMap.entrySet().iterator().forEachRemaining(expectedEntries::add);

		assertEquals(expectedEntries, actualEntries);
		assertBalanced(treeNode);
	}

	static <K,V> void assertBalanced(TreeNode<K,V> node) {
		if (!(node instanceof EmptyNode)) {
			OccupiedNode<K,V> tree = (OccupiedNode<K, V>) node;
			int leftSize = tree.left().size();
			int rightSize = tree.right().size();
			assertEquals(1 + leftSize + rightSize, tree.size());
			if (leftSize >= 1) {
				assertTrue(rightSize <= BALANCE_RATIO * leftSize);
			}
			if (rightSize >= 1) {
				assertTrue(leftSize <= BALANCE_RATIO * rightSize);
			}
		}
	}
}
