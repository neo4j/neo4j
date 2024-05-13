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
package org.neo4j.test.conditions;

import java.util.Collection;
import java.util.Objects;
import java.util.function.Predicate;
import org.assertj.core.api.Condition;
import org.neo4j.internal.helpers.collection.Iterables;

public final class Conditions {
    public static final Condition<Boolean> TRUE = new Condition<>(value -> value, "Should be true.");
    public static final Condition<Boolean> FALSE = new Condition<>(value -> !value, "Should be false.");

    private Conditions() {}

    public static <T> Condition<T> condition(Predicate<T> predicate) {
        Objects.requireNonNull(predicate);
        return new Condition<>(predicate, "Generic condition. See predicate for condition details.");
    }

    public static <T> Condition<Iterable<T>> contains(T value) {
        Objects.requireNonNull(value);
        return new Condition<>(v -> Iterables.stream(v).anyMatch(value::equals), "Should contain " + value);
    }

    public static <T> Condition<String> contains(String value) {
        Objects.requireNonNull(value);
        return new Condition<>(v -> v.contains(value), "Should contain string \"%s\"", value);
    }

    public static <T> Condition<T> equalityCondition(T value) {
        Objects.requireNonNull(value);
        return new Condition<>(value::equals, "Should be equal to " + value);
    }

    public static <T extends Comparable<T>> Condition<T> greaterThan(T value) {
        Objects.requireNonNull(value);
        return new Condition<>(v -> v.compareTo(value) > 0, "Should be greater than " + value);
    }

    public static <T extends Comparable<T>> Condition<T> greaterThanOrEqualTo(T value) {
        Objects.requireNonNull(value);
        return new Condition<>(v -> v.compareTo(value) >= 0, "Should be greater than or equal to " + value);
    }

    public static <T extends Comparable<T>> Condition<T> lessThan(T value) {
        Objects.requireNonNull(value);
        return new Condition<>(v -> v.compareTo(value) < 0, "Should be less than " + value);
    }

    public static <T extends Comparable<T>> Condition<T> lessThanOrEqualTo(T value) {
        Objects.requireNonNull(value);
        return new Condition<>(v -> v.compareTo(value) <= 0, "Should be less than or equal to " + value);
    }

    public static <T> Condition<T> instanceOf(Class<?> type) {
        Objects.requireNonNull(type);
        return new Condition<>(type::isInstance, "Should be instance of " + type.getName());
    }

    public static <T extends Collection<?>> Condition<T> sizeCondition(int expectedSize) {
        return new Condition<>(v -> v.size() == expectedSize, "Size should be equal to " + expectedSize);
    }
}
