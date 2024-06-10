/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.values.virtual;

import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;
import static org.neo4j.memory.HeapEstimator.sizeOf;
import static org.neo4j.memory.HeapEstimator.sizeOfHashMap;
import static org.neo4j.values.storable.Values.NO_VALUE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.StreamSupport;
import org.neo4j.function.ThrowingBiConsumer;
import org.neo4j.internal.helpers.collection.PrefetchingIterator;
import org.neo4j.values.AnyValue;
import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.Comparison;
import org.neo4j.values.Equality;
import org.neo4j.values.TernaryComparator;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.VirtualValue;
import org.neo4j.values.storable.Values;

public abstract class MapValue extends VirtualValue {
    public static final MapValue EMPTY = new MapValue() {
        @Override
        public Iterable<String> keySet() {
            return Collections.emptyList();
        }

        @Override
        public <E extends Exception> void foreach(ThrowingBiConsumer<String, AnyValue, E> f) {
            // do nothing
        }

        @Override
        public boolean entryExists(BiFunction<String, AnyValue, Boolean> p) {
            return false;
        }

        @Override
        public boolean containsKey(String key) {
            return false;
        }

        @Override
        public AnyValue get(String key) {
            return NO_VALUE;
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public boolean isEmpty() {
            return true;
        }

        @Override
        public long estimatedHeapUsage() {
            return 0L;
        }
    };

    private static final long MAP_WRAPPING_MAP_VALUE_SHALLOW_SIZE = shallowSizeOfInstance(MapWrappingMapValue.class);

    static final class MapWrappingMapValue extends MapValue {
        private final Map<String, AnyValue> map;
        private final long wrappedHeapSize;

        MapWrappingMapValue(Map<String, AnyValue> map, long payloadSize) {
            assert payloadSize >= 0;
            this.map = map;
            this.wrappedHeapSize = sizeOfHashMap(map) + payloadSize;
        }

        MapWrappingMapValue(Map<String, AnyValue> map, long mapSize, long payloadSize) {
            assert payloadSize >= 0;
            this.map = map;
            this.wrappedHeapSize = mapSize + payloadSize;
        }

        @Override
        public Iterable<String> keySet() {
            return map.keySet();
        }

        @Override
        public <E extends Exception> void foreach(ThrowingBiConsumer<String, AnyValue, E> f) throws E {
            for (Map.Entry<String, AnyValue> entry : map.entrySet()) {
                f.accept(entry.getKey(), entry.getValue());
            }
        }

        @Override
        public boolean entryExists(BiFunction<String, AnyValue, Boolean> p) {
            for (Map.Entry<String, AnyValue> entry : map.entrySet()) {
                if (p.apply(entry.getKey(), entry.getValue())) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean containsKey(String key) {
            return map.containsKey(key);
        }

        @Override
        public AnyValue get(String key) {
            return map.getOrDefault(key, NO_VALUE);
        }

        @Override
        public int size() {
            return map.size();
        }

        @Override
        public boolean isEmpty() {
            return map.isEmpty();
        }

        @Override
        public long estimatedHeapUsage() {
            return MAP_WRAPPING_MAP_VALUE_SHALLOW_SIZE + wrappedHeapSize;
        }
    }

    private static final long FILTERING_MAP_VALUE_SHALLOW_SIZE = shallowSizeOfInstance(FilteringMapValue.class);

    private static final class FilteringMapValue extends MapValue {
        private final MapValue map;
        private final BiFunction<String, AnyValue, Boolean> filter;
        private int size = -1;

        FilteringMapValue(MapValue map, BiFunction<String, AnyValue, Boolean> filter) {
            this.map = map;
            this.filter = filter;
        }

        @Override
        public Iterable<String> keySet() {
            List<String> keys = size >= 0 ? new ArrayList<>(size) : new ArrayList<>();
            foreach((key, value) -> {
                if (filter.apply(key, value)) {
                    keys.add(key);
                }
            });

            return keys;
        }

        @Override
        public <E extends Exception> void foreach(ThrowingBiConsumer<String, AnyValue, E> f) throws E {
            map.foreach((s, anyValue) -> {
                if (filter.apply(s, anyValue)) {
                    f.accept(s, anyValue);
                }
            });
        }

        @Override
        public boolean entryExists(BiFunction<String, AnyValue, Boolean> p) {
            return map.entryExists((s, anyValue) -> {
                if (filter.apply(s, anyValue)) {
                    return p.apply(s, anyValue);
                }
                return false;
            });
        }

        @Override
        public boolean containsKey(String key) {
            AnyValue value = map.get(key);
            if (value == NO_VALUE) {
                return false;
            } else {
                return filter.apply(key, value);
            }
        }

        @Override
        public AnyValue get(String key) {
            AnyValue value = map.get(key);
            if (value == NO_VALUE) {
                return NO_VALUE;
            } else if (filter.apply(key, value)) {
                return value;
            } else {
                return NO_VALUE;
            }
        }

        @Override
        public int size() {
            if (size < 0) {
                size = 0;
                foreach((k, v) -> {
                    if (filter.apply(k, v)) {
                        size++;
                    }
                });
            }
            return size;
        }

        @Override
        public boolean isEmpty() {
            return size() == 0;
        }

        @Override
        public long estimatedHeapUsage() {
            return FILTERING_MAP_VALUE_SHALLOW_SIZE + map.estimatedHeapUsage();
        }
    }

    private static final long MAPPED_MAP_VALUE_SHALLOW_SIZE = shallowSizeOfInstance(MappedMapValue.class);

    private static final class MappedMapValue extends MapValue {
        private final MapValue map;
        private final BiFunction<String, AnyValue, AnyValue> mapFunction;

        MappedMapValue(MapValue map, BiFunction<String, AnyValue, AnyValue> mapFunction) {
            this.map = map;
            this.mapFunction = mapFunction;
        }

        @Override
        public ListValue keys() {
            return map.keys();
        }

        @Override
        public Iterable<String> keySet() {
            return map.keySet();
        }

        @Override
        public <E extends Exception> void foreach(ThrowingBiConsumer<String, AnyValue, E> f) throws E {
            map.foreach((s, anyValue) -> f.accept(s, mapFunction.apply(s, anyValue)));
        }

        @Override
        public boolean entryExists(BiFunction<String, AnyValue, Boolean> p) {
            return map.entryExists((s, anyValue) -> p.apply(s, mapFunction.apply(s, anyValue)));
        }

        @Override
        public boolean containsKey(String key) {
            return map.containsKey(key);
        }

        @Override
        public AnyValue get(String key) {
            return mapFunction.apply(key, map.get(key));
        }

        @Override
        public int size() {
            return map.size();
        }

        @Override
        public boolean isEmpty() {
            return map.isEmpty();
        }

        @Override
        public long estimatedHeapUsage() {
            return MAPPED_MAP_VALUE_SHALLOW_SIZE + map.estimatedHeapUsage();
        }
    }

    private static final long UPDATED_MAP_VALUE_SHALLOW_SIZE = shallowSizeOfInstance(UpdatedMapValue.class);

    private static final class UpdatedMapValue extends MapValue {
        private final MapValue map;
        private final String updatedKey;
        private final AnyValue updatedValue;

        UpdatedMapValue(MapValue map, String updatedKey, AnyValue updatedValue) {
            assert !map.containsKey(updatedKey);
            this.map = map;
            this.updatedKey = updatedKey;
            this.updatedValue = updatedValue;
        }

        @Override
        public ListValue keys() {
            return VirtualValues.concat(map.keys(), VirtualValues.fromArray(Values.stringArray(updatedKey)));
        }

        @Override
        public Iterable<String> keySet() {
            return () -> new Iterator<>() {
                private Iterator<String> internal = map.keySet().iterator();
                private boolean hasNext = true;

                @Override
                public boolean hasNext() {
                    if (internal.hasNext()) {
                        return true;
                    } else {
                        return hasNext;
                    }
                }

                @Override
                public String next() {
                    if (internal.hasNext()) {
                        return internal.next();
                    } else if (hasNext) {
                        hasNext = false;
                        return updatedKey;
                    } else {
                        throw new NoSuchElementException();
                    }
                }
            };
        }

        @Override
        public <E extends Exception> void foreach(ThrowingBiConsumer<String, AnyValue, E> f) throws E {
            map.foreach(f);
            f.accept(updatedKey, updatedValue);
        }

        @Override
        public boolean entryExists(BiFunction<String, AnyValue, Boolean> p) {
            return map.entryExists(p) || p.apply(updatedKey, updatedValue);
        }

        @Override
        public boolean containsKey(String key) {
            if (updatedKey.equals(key)) {
                return true;
            }

            return map.containsKey(key);
        }

        @Override
        public AnyValue get(String key) {
            if (updatedKey.equals(key)) {
                return updatedValue;
            }

            return map.get(key);
        }

        @Override
        public int size() {
            return map.size() + 1;
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public long estimatedHeapUsage() {
            return UPDATED_MAP_VALUE_SHALLOW_SIZE
                    + map.estimatedHeapUsage()
                    + sizeOf(updatedKey)
                    + updatedValue.estimatedHeapUsage();
        }
    }

    private static final long COMBINED_MAP_VALUE_SHALLOW_SIZE = shallowSizeOfInstance(CombinedMapValue.class);

    private static final class CombinedMapValue extends MapValue {
        private final MapValue map1;
        private final MapValue map2;

        CombinedMapValue(MapValue map1, MapValue map2) {
            this.map1 = map1;
            this.map2 = map2;
        }

        @Override
        public Iterable<String> keySet() {
            return () -> new PrefetchingIterator<>() {
                private boolean iteratingMap2;
                private Iterator<String> iterator = map1.keySet().iterator();
                private Set<String> seen = new HashSet<>();

                @Override
                protected String fetchNextOrNull() {
                    while (!iteratingMap2 || iterator.hasNext()) {
                        if (!iterator.hasNext()) {
                            iterator = map2.keySet().iterator();
                            iteratingMap2 = true;
                        }

                        while (iterator.hasNext()) {
                            String key = iterator.next();
                            if (seen.add(key)) {
                                return key;
                            }
                        }
                    }
                    return null;
                }
            };
        }

        @Override
        public boolean entryExists(BiFunction<String, AnyValue, Boolean> p) {
            return map1.entryExists(p) || map2.entryExists(p);
        }

        @Override
        public <E extends Exception> void foreach(ThrowingBiConsumer<String, AnyValue, E> f) throws E {
            Set<String> seen = new HashSet<>();
            ThrowingBiConsumer<String, AnyValue, E> consume = (key, value) -> {
                if (seen.add(key)) {
                    f.accept(key, value);
                }
            };
            map2.foreach(consume);
            map1.foreach(consume);
        }

        @Override
        public boolean containsKey(String key) {
            if (map1.containsKey(key)) {
                return true;
            }
            return map2.containsKey(key);
        }

        @Override
        public AnyValue get(String key) {
            AnyValue value2 = map2.get(key);
            if (value2 != NO_VALUE) {
                return value2;
            }
            return map1.get(key);
        }

        @Override
        public int size() {
            int[] size = {0};
            Set<String> seen = new HashSet<>();
            ThrowingBiConsumer<String, AnyValue, RuntimeException> consume = (key, value) -> {
                if (seen.add(key)) {
                    size[0]++;
                }
            };
            map1.foreach(consume);
            map2.foreach(consume);
            return size[0];
        }

        @Override
        public boolean isEmpty() {
            return map1.isEmpty() && map2.isEmpty();
        }

        @Override
        public long estimatedHeapUsage() {
            return COMBINED_MAP_VALUE_SHALLOW_SIZE + map1.estimatedHeapUsage() + map2.estimatedHeapUsage();
        }
    }

    @Override
    protected int computeHashToMemoize() {
        int[] h = new int[1];
        foreach((key, value) -> h[0] += key.hashCode() ^ value.hashCode());
        return h[0];
    }

    @Override
    public <E extends Exception> void writeTo(AnyValueWriter<E> writer) throws E {
        writer.beginMap(size());
        foreach((s, anyValue) -> {
            writer.writeString(s);
            anyValue.writeTo(writer);
        });
        writer.endMap();
    }

    @Override
    public boolean equals(VirtualValue other) {
        if (!(other instanceof MapValue that)) {
            return false;
        }
        int size = size();
        if (size != that.size()) {
            return false;
        }

        Iterable<String> keys = keySet();
        for (String key : keys) {
            if (!get(key).equals(that.get(key))) {
                return false;
            }
        }

        return true;
    }

    public abstract Iterable<String> keySet();

    public ListValue keys() {
        String[] keys = new String[size()];
        int i = 0;
        for (String key : keySet()) {
            keys[i++] = key;
        }
        return VirtualValues.fromArray(Values.stringArray(keys));
    }

    @Override
    public VirtualValueGroup valueGroup() {
        return VirtualValueGroup.MAP;
    }

    @Override
    public int unsafeCompareTo(VirtualValue other, Comparator<AnyValue> comparator) {
        MapValue otherMap = (MapValue) other;
        int size = size();
        int compare = Integer.compare(size, otherMap.size());
        if (compare == 0) {
            String[] thisKeys =
                    StreamSupport.stream(keySet().spliterator(), false).toArray(String[]::new);
            Arrays.sort(thisKeys, String::compareTo);
            String[] thatKeys =
                    StreamSupport.stream(otherMap.keySet().spliterator(), false).toArray(String[]::new);
            Arrays.sort(thatKeys, String::compareTo);
            for (int i = 0; i < size; i++) {
                compare = thisKeys[i].compareTo(thatKeys[i]);
                if (compare != 0) {
                    return compare;
                }
            }

            for (int i = 0; i < size; i++) {
                String key = thisKeys[i];
                compare = comparator.compare(get(key), otherMap.get(key));
                if (compare != 0) {
                    return compare;
                }
            }
        }
        return compare;
    }

    @Override
    public Comparison unsafeTernaryCompareTo(VirtualValue other, TernaryComparator<AnyValue> comparator) {
        MapValue otherMap = (MapValue) other;
        int size = size();
        int compare = Integer.compare(size, otherMap.size());
        if (compare == 0) {
            String[] thisKeys =
                    StreamSupport.stream(keySet().spliterator(), false).toArray(String[]::new);
            Arrays.sort(thisKeys, String::compareTo);
            String[] thatKeys =
                    StreamSupport.stream(otherMap.keySet().spliterator(), false).toArray(String[]::new);
            Arrays.sort(thatKeys, String::compareTo);
            for (int i = 0; i < size; i++) {
                compare = thisKeys[i].compareTo(thatKeys[i]);
                if (compare != 0) {
                    return Comparison.from(compare);
                }
            }

            for (int i = 0; i < size; i++) {
                String key = thisKeys[i];
                Comparison comparison = comparator.ternaryCompare(get(key), otherMap.get(key));
                if (comparison != Comparison.EQUAL) {
                    return comparison;
                }
            }
        }
        return Comparison.from(compare);
    }

    @Override
    public Equality ternaryEquals(AnyValue other) {
        assert other != null : "null values are not supported, use NoValue.NO_VALUE instead";
        if (other == NO_VALUE) {
            return Equality.UNDEFINED;
        } else if (!(other instanceof MapValue)) {
            return Equality.FALSE;
        }
        MapValue otherMap = (MapValue) other;
        int size = size();
        if (size != otherMap.size()) {
            return Equality.FALSE;
        }
        String[] thisKeys = StreamSupport.stream(keySet().spliterator(), false).toArray(String[]::new);
        Arrays.sort(thisKeys, String::compareTo);
        String[] thatKeys =
                StreamSupport.stream(otherMap.keySet().spliterator(), false).toArray(String[]::new);
        Arrays.sort(thatKeys, String::compareTo);
        for (int i = 0; i < size; i++) {
            if (thisKeys[i].compareTo(thatKeys[i]) != 0) {
                return Equality.FALSE;
            }
        }
        Equality equalityResult = Equality.TRUE;
        for (int i = 0; i < size; i++) {
            String key = thisKeys[i];
            AnyValue thisValue = get(key);
            AnyValue otherValue = otherMap.get(key);

            Equality equality = thisValue.ternaryEquals(otherValue);
            if (equality == Equality.UNDEFINED) {
                equalityResult = Equality.UNDEFINED;
            } else if (equality == Equality.FALSE) {
                return Equality.FALSE;
            }
        }
        return equalityResult;
    }

    @Override
    public <T> T map(ValueMapper<T> mapper) {
        return mapper.mapMap(this);
    }

    public abstract <E extends Exception> void foreach(ThrowingBiConsumer<String, AnyValue, E> f) throws E;

    public abstract boolean entryExists(BiFunction<String, AnyValue, Boolean> p);

    public abstract boolean containsKey(String key);

    public abstract AnyValue get(String key);

    public abstract int size();

    public abstract boolean isEmpty();

    public MapValue filter(BiFunction<String, AnyValue, Boolean> filterFunction) {
        return new FilteringMapValue(this, filterFunction);
    }

    public MapValue updatedWith(String key, AnyValue value) {
        if (!containsKey(key)) {
            return new UpdatedMapValue(this, key, value);
        } else {
            AnyValue current = get(key);
            if (current.equals(value)) {
                return this;
            } else {
                return new MappedMapValue(this, (k, v) -> {
                    if (k.equals(key)) {
                        return value;
                    } else {
                        return v;
                    }
                });
            }
        }
    }

    public MapValue updatedWith(MapValue other) {
        return new CombinedMapValue(this, other);
    }

    @Override
    public String getTypeName() {
        return "Map";
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(getTypeName()).append('{');
        final String[] sep = new String[] {""};
        foreach((key, value) -> {
            sb.append(sep[0]);
            sb.append(key);
            sb.append(" -> ");
            sb.append(value);
            sep[0] = ", ";
        });
        sb.append('}');
        return sb.toString();
    }
}
