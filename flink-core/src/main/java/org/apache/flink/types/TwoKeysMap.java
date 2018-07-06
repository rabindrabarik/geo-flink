package org.apache.flink.types;

import org.apache.commons.lang3.tuple.Pair;

import java.util.Set;

/**
 * Map with two keys of type {@link K}, {@link L} for each value.
 */
public interface TwoKeysMap<K, L, V> {

	// Basic querying //

	int size();

	boolean isEmpty();

	boolean containsValue(V value);

	boolean containsKey(K key1, L key2);


	// Basic operations //

	V get(K key1, L key2);

	V put(K key1, L key2, V value);

	V remove(K key1, L key2);

	void clear();

	V getOrDefault(K key1, L key2, V defaultValue);


	// Collections //

	Set<Pair<K, L>> keySet();

	Set<Entry<K, L, V>> entrySet();


	// Entry inner class //

	class Entry<K, L, V> {
		private K key1;
		private L key2;
		private V value;

		public Entry(K key1, L key2, V value) {
			this.key1 = key1;
			this.key2 = key2;
			this.value = value;
		}

		public K getKey1() {
			return key1;
		}

		public L getKey2() {
			return key2;
		}

		public V getValue() {
			return value;
		}
	}
}
