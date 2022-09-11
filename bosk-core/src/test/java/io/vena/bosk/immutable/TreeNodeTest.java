package io.vena.bosk.immutable;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.stream.IntStream;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import static io.vena.bosk.immutable.OccupiedNode.BALANCE_RATIO;
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
	void ascendingOrder_expectedItems() {
		assertEquivalentToTreeMap(
			entryList(IntStream.rangeClosed(1, 3)),
			String::compareTo);
	}

	@Test
	void descendingOrder_expectedItems() {
		assertEquivalentToTreeMap(
			entryList(IntStream.rangeClosed(3, 1)),
			String::compareTo);
	}

	@Test
	void mixedOrder_expectedItems() {
		assertEquivalentToTreeMap(
			// This order was observed to trigger a bug
			entryList(IntStream.of(850536, 447530, 534792, 231632, 718173, 704060)),
			String::compareTo);
	}

	@Test
	void randomList_expectedItems() {
		assertEquivalentToTreeMap(
			entryList(new Random(123).ints(10000, 0, 1_000_000)),
			String::compareTo
		);
	}

	@NotNull
	private static List<Map.Entry<String, String>> entryList(IntStream numbers) {
		return numbers
			.mapToObj(i -> String.format("%012d", i))
			.map(s -> new SimpleEntry<>("key" + s, "value" + s))
			.collect(toList());
	}

	private static TreeNode<String, String> empty() {
		return TreeNode.empty();
	}

	private static <K,V> void assertEquivalentToTreeMap(Iterable<Map.Entry<K, V>> entries, Comparator<K> comparator) {
		TreeNode<K,V> treeNode = TreeNode.<K,V>empty();
		for (Map.Entry<K, V> entry: entries) {
			treeNode = treeNode.with(entry.getKey(), entry.getValue(), comparator);
		}

		TreeMap<K,V> treeMap = new TreeMap<>(comparator);
		for (Map.Entry<K, V> entry: entries) {
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
			assertTrue(leftSize <= BALANCE_RATIO * rightSize);
			assertTrue(rightSize <= BALANCE_RATIO * leftSize);
		}
	}
}
