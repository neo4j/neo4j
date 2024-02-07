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
package org.neo4j.logging.event;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;

/**
 * This class is used by the {@link LoggingDebugEventPublisher} for parameter logging. Because some maps in
 * java does not allow null as values, this if for example true for the most convenient map {@link Map#of()}, there
 * is a risk of the event publisher throwing {@link NullPointerException}. It would be unexpected and unacceptable
 * for the {@link DebugEventPublisher} to throw on null values.
 */
public class Parameters {

    static final Parameters EMPTY = new Parameters(new Object[0]);
    private final Object[] parameters;

    public static Parameters of(String key, Object value) {
        return params(key, value);
    }

    public static Parameters of(String key1, Object value1, String key2, Object value2) {
        return params(key1, value1, key2, value2);
    }

    public static Parameters of(String key1, Object value1, String key2, Object value2, String key3, Object value3) {
        return params(key1, value1, key2, value2, key3, value3);
    }

    public static Parameters of(
            String key1,
            Object value1,
            String key2,
            Object value2,
            String key3,
            Object value3,
            String key4,
            Object value4) {
        return params(key1, value1, key2, value2, key3, value3, key4, value4);
    }

    public static Parameters of(
            String key1,
            Object value1,
            String key2,
            Object value2,
            String key3,
            Object value3,
            String key4,
            Object value4,
            String key5,
            Object value5) {
        return params(key1, value1, key2, value2, key3, value3, key4, value4, key5, value5);
    }

    public static Parameters of(
            String key1,
            Object value1,
            String key2,
            Object value2,
            String key3,
            Object value3,
            String key4,
            Object value4,
            String key5,
            Object value5,
            String key6,
            Object value6) {
        return params(key1, value1, key2, value2, key3, value3, key4, value4, key5, value5, key6, value6);
    }

    private static Parameters params(Object... keyValues) {
        if (keyValues.length % 2 != 0) {
            throw new IllegalArgumentException("Expected an even sized array of parameters");
        }
        return new Parameters(keyValues);
    }

    private Parameters(Object[] params) {
        this.parameters = params;
    }

    public static Parameters concat(Parameters param1, Parameters param2) {
        return new Parameters(Stream.concat(Arrays.stream(param1.parameters), Arrays.stream(param2.parameters))
                .toArray(Object[]::new));
    }

    @Override
    public String toString() {
        if (parameters.length == 0) {
            return "";
        }
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("[");
        for (int i = 0; i < parameters.length; i += 2) {
            stringBuilder.append(parameters[i]);
            stringBuilder.append("=");
            stringBuilder.append(parameters[i + 1]);
            if (i + 2 != parameters.length) {
                stringBuilder.append(", ");
            }
        }
        stringBuilder.append("]");
        return stringBuilder.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Parameters that)) {
            return false;
        }

        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        return Arrays.equals(parameters, that.parameters);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(parameters);
    }

    public boolean isEmpty() {
        return parameters.length == 0;
    }
}
