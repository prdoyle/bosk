package io.vena.bosk.immutable;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;

interface TreeNode<K, V> {
	int size();
	TreeNode<K,V> with(K key, V value, Comparator<K> comparator);
	TreeNode<K,V> without(K key, Comparator<K> comparator);
	TreeNode<K,V> withAll(TreeNode<K,V> other, Comparator<K> comparator);
	TreeNode<K,V> withoutAll(TreeNode<K,V> other, Comparator<K> comparator);

	Iterator<Map.Entry<K,V>> entryIterator();

	static <KK,VV> TreeNode<KK,VV> empty() {
		return EmptyNode.instance();
	}

	static <KK,VV> TreeNode<KK,VV> of(KK key, VV value) {
		return TreeNode.<KK,VV>empty().with(key, value, (a,b)->0); // Comparator doesn't matter
	}
}
