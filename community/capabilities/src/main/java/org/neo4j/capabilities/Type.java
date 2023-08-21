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
package org.neo4j.capabilities;

import java.util.Collection;
import java.util.Objects;

/**
 * Defines types a capability value can have.
 *
 * @param <T> the java type that this class represents.
 */
public class Type<T> {
    private final String name;
    private final String description;
    private final Class<T> type;

    private Type(String name, String description, Class<T> type) {
        this.name = Objects.requireNonNull(name);
        this.description = Objects.requireNonNull(description);
        this.type = Objects.requireNonNull(type);
    }

    String name() {
        return name;
    }

    String description() {
        return this.description;
    }

    Class<T> type() {
        return type;
    }

    @Override
    public String toString() {
        return "Type{" + "name='" + name + '\'' + '}';
    }

    /**
     * Type that holds a String value.
     */
    public static final Type<String> STRING = new Type<>("string", "a string value", String.class);
    /**
     * Type that holds a Boolean value.
     */
    public static final Type<Boolean> BOOLEAN = new Type<>("boolean", "a boolean value", Boolean.class);
    /**
     * Type that holds a Integer value.
     */
    public static final Type<Integer> INTEGER = new Type<>("integer", "an integer value", Integer.class);
    /**
     * Type that holds a Long value.
     */
    public static final Type<Long> LONG = new Type<>("long", "a long value", Long.class);
    /**
     * Type that holds a Float value.
     */
    public static final Type<Float> FLOAT = new Type<>("float", "a float value", Float.class);
    /**
     * Type that holds a Double value.
     */
    public static final Type<Double> DOUBLE = new Type<>("double", "a double value", Double.class);

    /**
     * Generates a type that is a list of the provided simple type.
     *
     * @param type the element type.
     * @param <T>  the java type that the element type represents.
     * @return the list type.
     * @throws IllegalArgumentException when the provided type is already a list.
     */
    @SuppressWarnings("unchecked")
    public static <T> Type<Collection<T>> listOf(Type<T> type) {
        if (Collection.class.isAssignableFrom(type.type())) {
            throw new IllegalArgumentException("nested list types is not supported.");
        }

        return new Type<>(
                String.format("list of %s", type.name()),
                String.format("a list of %s values", type.name()),
                (Class<Collection<T>>) (Object) Collection.class);
    }
}
