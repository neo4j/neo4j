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
package org.neo4j.shell.test.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.value.IntegerValue;
import org.neo4j.driver.types.Entity;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Path;
import org.neo4j.driver.types.Relationship;
import org.neo4j.driver.util.Pair;
import org.neo4j.shell.test.Util;

/**
 * A fake record of fake values
 */
public class FakeRecord implements Record {

    private final TreeMap<String, Value> valueMap = new TreeMap<>();

    public static FakeRecord of(String key, String value) {
        return of(key, new FakeValue() {
            @Override
            public Value get(String key, Value defaultValue) {
                return null;
            }

            @Override
            public Object get(String key, Object defaultValue) {
                return null;
            }

            @Override
            public Number get(String key, Number defaultValue) {
                return null;
            }

            @Override
            public Entity get(String key, Entity defaultValue) {
                return null;
            }

            @Override
            public Node get(String key, Node defaultValue) {
                return null;
            }

            @Override
            public Path get(String key, Path defaultValue) {
                return null;
            }

            @Override
            public Relationship get(String key, Relationship defaultValue) {
                return null;
            }

            @Override
            public List<Object> get(String key, List<Object> defaultValue) {
                return null;
            }

            @Override
            public Map<String, Object> get(String key, Map<String, Object> defaultValue) {
                return null;
            }

            @Override
            public int get(String key, int defaultValue) {
                return 0;
            }

            @Override
            public long get(String key, long defaultValue) {
                return 0;
            }

            @Override
            public boolean get(String key, boolean defaultValue) {
                return false;
            }

            @Override
            public String get(String key, String defaultValue) {
                return null;
            }

            @Override
            public float get(String key, float defaultValue) {
                return 0;
            }

            @Override
            public double get(String key, double defaultValue) {
                return 0;
            }

            @Override
            public Object asObject() {
                return value;
            }

            @Override
            public String asString() {
                return value;
            }
        });
    }

    public static FakeRecord of(String key, Value value) {
        FakeRecord record = new FakeRecord();
        record.valueMap.put(key, value);

        return record;
    }

    public static FakeRecord of(Map<String, Value> values) {
        FakeRecord record = new FakeRecord();
        record.valueMap.putAll(values);

        return record;
    }

    @Override
    public List<String> keys() {
        return new ArrayList<>(valueMap.keySet());
    }

    @Override
    public List<Value> values() {
        return new ArrayList<>(valueMap.values());
    }

    @Override
    public <T> Iterable<T> values(Function<Value, T> function) {
        return () -> valueMap.values().stream().map(function).iterator();
    }

    @Override
    public boolean containsKey(String key) {
        return valueMap.containsKey(key);
    }

    @Override
    public int index(String key) {
        return keys().indexOf(key);
    }

    @Override
    public Value get(String key) {
        return valueMap.get(key);
    }

    @Override
    public Value get(int index) {
        return valueMap.get(keys().get(index));
    }

    @Override
    public int size() {
        return valueMap.size();
    }

    @Override
    public Map<String, Object> asMap() {
        throw new Util.NotImplementedYetException("Not implemented as no test has required it yet");
    }

    @Override
    public <T> Map<String, T> asMap(Function<Value, T> mapper) {
        throw new Util.NotImplementedYetException("Not implemented as no test has required it yet");
    }

    @Override
    public List<Pair<String, Value>> fields() {
        throw new Util.NotImplementedYetException("Not implemented as no test has required it yet");
    }

    @Override
    public Value get(String key, Value defaultValue) {
        return null;
    }

    @Override
    public Object get(String key, Object defaultValue) {
        return null;
    }

    @Override
    public Number get(String key, Number defaultValue) {
        return null;
    }

    @Override
    public Entity get(String key, Entity defaultValue) {
        return null;
    }

    @Override
    public Node get(String key, Node defaultValue) {
        return null;
    }

    @Override
    public Path get(String key, Path defaultValue) {
        return null;
    }

    @Override
    public Relationship get(String key, Relationship defaultValue) {
        return null;
    }

    @Override
    public List<Object> get(String key, List<Object> defaultValue) {
        return null;
    }

    @Override
    public <T> List<T> get(String key, List<T> defaultValue, Function<Value, T> mapFunc) {
        return null;
    }

    @Override
    public Map<String, Object> get(String key, Map<String, Object> defaultValue) {
        return null;
    }

    @Override
    public <T> Map<String, T> get(String key, Map<String, T> defaultValue, Function<Value, T> mapFunc) {
        return null;
    }

    @Override
    public int get(String key, int defaultValue) {
        return 0;
    }

    @Override
    public long get(String key, long defaultValue) {
        return valueMap.getOrDefault(key, new IntegerValue(defaultValue)).asLong();
    }

    @Override
    public boolean get(String key, boolean defaultValue) {
        return false;
    }

    @Override
    public String get(String key, String defaultValue) {
        return null;
    }

    @Override
    public float get(String key, float defaultValue) {
        return 0;
    }

    @Override
    public double get(String key, double defaultValue) {
        return 0;
    }
}
