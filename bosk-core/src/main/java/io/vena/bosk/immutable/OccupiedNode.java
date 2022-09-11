package io.vena.bosk.immutable;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import lombok.Value;
import lombok.experimental.Accessors;

import static java.util.Collections.singletonMap;

@Value
@Accessors(fluent = true)
class OccupiedNode<K,V> implements TreeNode<K,V> {
	int size;
	K key;
	V value;
	TreeNode<K,V> left, right;

	public OccupiedNode(K key, V value, TreeNode<K, V> left, TreeNode<K, V> right) {
		this.key = key;
		this.value = value;
		this.left = left;
		this.right = right;

		this.size = 1 + left.size() + right.size();
	}

	@Override
	public int size() {
		return size;
	}

	@Override
	public TreeNode<K, V> with(K givenKey, V givenValue, Comparator<K> comparator) {
		int discriminator = comparator.compare(givenKey, this.key);
		if (discriminator < 0) {
			return balanced(
				this.key, this.value,
				left.with(givenKey, givenValue, comparator),
				right
			);
		} else if (discriminator > 0) {
			return balanced(
				this.key, this.value,
				left,
				right.with(givenKey, givenValue, comparator)
			);
		} else {
			return new OccupiedNode<>(
				this.key, // Retain existing key object
				givenValue,
				left, right
			);
		}
	}

	@Override
	public TreeNode<K, V> without(K givenKey, Comparator<K> comparator) {
		int discriminator = comparator.compare(givenKey, this.key);
		if (discriminator < 0) {
			return balanced(
				this.key, this.value,
				left.without(givenKey, comparator),
				right
			);
		} else if (discriminator > 0) {
			return balanced(
				this.key, this.value,
				left,
				right.without(givenKey, comparator)
			);
		}

		// We're deleting this node

		if (left.size() == 0) {
			return right;
		} else if (right.size() == 0) {
			return left;
		}

		// Now we know both children are occupied
		OccupiedNode<K,V> rightTree = (OccupiedNode<K, V>) right;
		OccupiedNode<K,V> newRoot = rightTree.leftmostDescendant();

		return balanced(
			newRoot.key, newRoot.value,
			left,
			right.without(newRoot.key, comparator)
		);
	}

	@Override
	public TreeNode<K, V> union(TreeNode<K, V> other, Comparator<K> comparator) {
		return null;
	}

	@Override
	public TreeNode<K, V> difference(TreeNode<K, V> other, Comparator<K> comparator) {
		return null;
	}

	@Override
	public TreeNode<K, V> intersection(TreeNode<K, V> other, Comparator<K> comparator) {
		return null;
	}

	@Override
	public Iterator<Map.Entry<K, V>> entryIterator() {
		Iterator<Map.Entry<K, V>> leftIter = left.entryIterator();
		Iterator<Map.Entry<K, V>> selfIter = singletonMap(key, value).entrySet().iterator();
		Iterator<Map.Entry<K, V>> rightIter = right.entryIterator();
		return new Iterator<Map.Entry<K, V>>() {
			@Override
			public boolean hasNext() {
				return leftIter.hasNext() || selfIter.hasNext() || rightIter.hasNext();
			}

			@Override
			public Map.Entry<K, V> next() {
				if (leftIter.hasNext()) {
					return leftIter.next();
				} else if (selfIter.hasNext()) {
					return selfIter.next();
				} else {
					return rightIter.next();
				}
			}
		};
	}

	public OccupiedNode<K,V> leftmostDescendant() {
		if (left.size() == 0) {
			return this;
		} else {
			OccupiedNode<K,V> leftTree = (OccupiedNode<K, V>) left;
			return leftTree.leftmostDescendant();
		}
	}

	private static <KK,VV> TreeNode<KK,VV> balanced(KK key, VV value, TreeNode<KK,VV> left, TreeNode<KK,VV> right) {
		int ln = left.size();
		int rn = right.size();
		if (ln+rn < 2) {
			return new OccupiedNode<>(key, value, left, right);
		}

		if (rn > BALANCE_RATIO * ln) {
			OccupiedNode<KK,VV> rightTree = (OccupiedNode<KK, VV>) right;
			TreeNode<KK, VV> rl = rightTree.left();
			TreeNode<KK, VV> rr = rightTree.right();
			int rln = rl.size();
			int rrn = rr.size();
			if (rln < rrn) {
				return single_L(
					key, value,
					rightTree.key, rightTree.value,
					left, rl, rr);
			} else {
				OccupiedNode<KK,VV> rlTree = (OccupiedNode<KK, VV>) rightTree.left;
				return doubleRotation(
					key, value,
					rlTree.key, rlTree.value,
					rightTree.key, rightTree.value,
					left, rlTree.left, rlTree.right, rr
				);
			}
		} else if (ln > BALANCE_RATIO * rn) {
			OccupiedNode<KK,VV> leftTree = (OccupiedNode<KK, VV>) left;
			TreeNode<KK, VV> ll = leftTree.left();
			TreeNode<KK, VV> lr = leftTree.right();
			int lln = ll.size();
			int lrn = lr.size();
			if (lrn < lln) {
				return single_R(
					leftTree.key, leftTree.value,
					key, value,
					ll, lr, right);
			} else {
				OccupiedNode<KK,VV> lrTree = (OccupiedNode<KK, VV>) leftTree.right;
				return doubleRotation(
					leftTree.key, leftTree.value,
					lrTree.key, lrTree.value,
					key, value,
					ll, lrTree.left, lrTree.right, right
				);
			}
		} else {
			return new OccupiedNode<>(key, value, left, right);
		}
	}

	private static <KK,VV> TreeNode<KK,VV> single_L(
		KK aKey, VV aValue,
		KK bKey, VV bValue,
		TreeNode<KK,VV> x, TreeNode<KK,VV> y, TreeNode<KK,VV> z
	) {
		return new OccupiedNode<>(
			bKey, bValue,
			new OccupiedNode<>(aKey, aValue, x, y),
			z);
	}

	private static <KK,VV> TreeNode<KK,VV> single_R(
		KK aKey, VV aValue,
		KK bKey, VV bValue,
		TreeNode<KK,VV> x, TreeNode<KK,VV> y, TreeNode<KK,VV> z
	) {
		return new OccupiedNode<>(
			bKey, bValue,
			new OccupiedNode<>(aKey, aValue, x, y),
			z);
	}

	private static <KK,VV> TreeNode<KK,VV> doubleRotation(
		KK aKey, VV aValue,
		KK bKey, VV bValue,
		KK cKey, VV cValue,
		TreeNode<KK,VV> x, TreeNode<KK,VV> y1, TreeNode<KK,VV> y2, TreeNode<KK,VV> z
	) {
		return new OccupiedNode<>(
			bKey, bValue,
			new OccupiedNode<>(aKey, aValue, x, y1),
			new OccupiedNode<>(cKey, cValue, y2, z));
	}

	static final int BALANCE_RATIO = 5;
}
