package org.apache.flink.types;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TwoKeysMultiMap<K, L, V> implements TwoKeysMap<K, L, V> {
	private Map<K, Map<L, V>> multiMap = new HashMap<>();

	private int size = 0;

	@Override
	public int size() {
		return size;
	}

	@Override
	public boolean isEmpty() {
		return size == 0;
	}

	@Override
	public boolean containsValue(V value) {
		for (Map.Entry<K, Map<L, V>> outerEntry : multiMap.entrySet()) {
			Map<L, V> innerMap = outerEntry.getValue();
			if(innerMap != null && innerMap.containsValue(value)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public boolean containsKey(K key1, L key2) {
		if(multiMap.containsKey(key1)) {
			return multiMap.get(key1).containsKey(key2);
		}
		return false;
	}

	@Override
	public V get(K key1, L key2) {
		Map<L, V> innerMap = multiMap.get(key1);
		if(innerMap == null) {
			return null;
		} else {
			return innerMap.get(key2);
		}
	}

	@Override
	public V put(K key1, L key2, V value) {
		if(!multiMap.containsKey(key1)) {
			multiMap.put(key1, new HashMap<>());
		}

		return multiMap.get(key1).put(key2, value);
	}

	@Override
	public V remove(K key1, L key2) {
		Map<L, V> innerMap = multiMap.get(key1);
		if(innerMap == null) {
			return null;
		} else {
			return innerMap.remove(key2);
		}
	}

	@Override
	public void clear() {
		multiMap.clear();
	}

	@Override
	public V getOrDefault(K key1, L key2, V defaultValue) {
		V contained = get(key1, key2);
		if(contained == null) {
			return defaultValue;
		} else {
			return contained;
		}
	}

	@Override
	public Set<Pair<K, L>> keySet() {
		Set<Pair<K, L>> output = new HashSet<>();

		for (Map.Entry<K, Map<L, V>> outerEntry : multiMap.entrySet()) {
			for (Map.Entry<L, V> innerEntry : outerEntry.getValue().entrySet()) {
				output.add(new ImmutablePair<>(outerEntry.getKey(), innerEntry.getKey()));
			}
		}

		return output;
	}

	@Override
	public Set<Entry<K, L, V>> entrySet() {
		Set<Entry<K, L, V>> output = new HashSet<>();

		for (Map.Entry<K, Map<L, V>> outerEntry : multiMap.entrySet()) {
			for (Map.Entry<L, V> innerEntry : outerEntry.getValue().entrySet()) {
				output.add(new Entry<>(outerEntry.getKey(), innerEntry.getKey(), innerEntry.getValue()));
			}
		}

		return output;
	}
}
