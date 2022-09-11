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
	public TreeNode<K, V> withAll(TreeNode<K, V> other, Comparator<K> comparator) {
		if (other.size() == 0) {
			return this;
		}
		OccupiedNode<K,V> otherTree = (OccupiedNode<K, V>) other;

		TreeNode<K,V> leftPart = split_lt(other, this.key, comparator);
		TreeNode<K,V> rightPart = split_gt(other, this.key, comparator);

		// Other node's value takes precedence
		V resultValue;
		if (this.key.equals(otherTree.key)) {
			resultValue = otherTree.value;
		} else {
			resultValue = this.value;
		}

		return concat3(
			this.key, // Existing key takes precedence
			resultValue,
			this.left.withAll(leftPart, comparator),
			this.right.withAll(rightPart, comparator),
			comparator
		);
	}

	@Override
	public TreeNode<K, V> withoutAll(TreeNode<K, V> other, Comparator<K> comparator) {
		if (other.size() == 0) {
			return this;
		}
		OccupiedNode<K,V> otherTree = (OccupiedNode<K, V>) other;

		TreeNode<K,V> leftPart = split_lt(other, this.key, comparator);
		TreeNode<K,V> rightPart = split_gt(other, this.key, comparator);
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
				// single_L
				return new OccupiedNode<>(
					rightTree.key, rightTree.value,
					new OccupiedNode<>(key, value, left, rl),
					rr);
			} else {
				// double_L
				OccupiedNode<KK,VV> rlTree = (OccupiedNode<KK, VV>) rightTree.left;
				return new OccupiedNode<>(
					rlTree.key, rlTree.value,
					new OccupiedNode<>(key, value, left, rlTree.left),
					new OccupiedNode<>(rightTree.key, rightTree.value, rlTree.right, rr));
			}
		} else if (ln > BALANCE_RATIO * rn) {
			OccupiedNode<KK,VV> leftTree = (OccupiedNode<KK, VV>) left;
			TreeNode<KK, VV> ll = leftTree.left();
			TreeNode<KK, VV> lr = leftTree.right();
			int lln = ll.size();
			int lrn = lr.size();
			if (lrn < lln) {
				// single_R
				return new OccupiedNode<>(
					leftTree.key, leftTree.value,
					ll,
					new OccupiedNode<>(key, value, lr, right));
			} else {
				// double_R
				OccupiedNode<KK,VV> lrTree = (OccupiedNode<KK, VV>) leftTree.right;
				return new OccupiedNode<>(
					lrTree.key, lrTree.value,
					new OccupiedNode<>(leftTree.key, leftTree.value, ll, lrTree.left),
					new OccupiedNode<>(key, value, lrTree.right, right));
			}
		} else {
			return new OccupiedNode<>(key, value, left, right);
		}
	}

	private static <KK,VV> TreeNode<KK,VV> concat3(KK key, VV value, TreeNode<KK,VV> left, TreeNode<KK,VV> right, Comparator<KK> comparator) {
		int n1 = left.size();
		int n2 = right.size();
		if (n1 == 0) {
			return right.with(key, value, comparator);
		} else if (n2 == 0) {
			return left.with(key, value, comparator);
		}

		OccupiedNode<KK,VV> leftTree = (OccupiedNode<KK, VV>) left;
		OccupiedNode<KK,VV> rightTree = (OccupiedNode<KK, VV>) right;

		if (BALANCE_RATIO * n1 < n2) {
			return balanced(
				rightTree.key, rightTree.value,
				concat3(key, value, left, rightTree.left, comparator),
				rightTree.right);
		} else if (BALANCE_RATIO * n2 < n1) {
			return balanced(
				leftTree.key, leftTree.value,
				leftTree.left,
				concat3(key, value, leftTree.right, right, comparator));
		} else {
			return new OccupiedNode<>(key, value, left, right);
		}
	}

	private static <KK,VV> TreeNode<KK,VV> split_lt(TreeNode<KK,VV> node, KK key, Comparator<KK> comparator) {
		if (node.size() == 0) {
			return node;
		}

		OccupiedNode<KK,VV> tree = (OccupiedNode<KK, VV>) node;
		int discriminator = comparator.compare(key, tree.key);
		if (discriminator < 0) {
			return split_lt(tree.left, key, comparator);
		} else if (discriminator > 0) {
			return concat3(tree.key, tree.value, tree.left, split_lt(tree.right, key, comparator), comparator);
		} else {
			return tree.left;
		}
	}

	private static <KK,VV> TreeNode<KK,VV> split_gt(TreeNode<KK,VV> node, KK key, Comparator<KK> comparator) {
		if (node.size() == 0) {
			return node;
		}

		OccupiedNode<KK,VV> tree = (OccupiedNode<KK, VV>) node;
		int discriminator = comparator.compare(tree.key, key);
		if (discriminator < 0) {
			return split_gt(tree.right, key, comparator);
		} else if (discriminator > 0) {
			return concat3(tree.key, tree.value, split_gt(tree.left, key, comparator), tree.right, comparator);
		} else {
			return tree.right;
		}
	}

	static final int BALANCE_RATIO = 5;
}
