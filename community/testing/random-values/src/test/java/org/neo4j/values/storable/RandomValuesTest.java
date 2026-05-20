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
package org.neo4j.values.storable;

import static java.lang.Character.isAlphabetic;
import static java.lang.Character.isDigit;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.neo4j.values.storable.Values.ZERO_INT;
import static org.neo4j.values.storable.Values.longValue;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.function.Predicates;
import org.neo4j.values.AnyValue;

abstract class RandomValuesTest {
    private static final int ITERATIONS = 500;
    private static final Duration TIMEOUT = Duration.ofMinutes(2);

    private RandomValues randomValues;

    private static final byte BOUND = 100;
    private static final LongValue UPPER = longValue(BOUND);
    private static final Set<Class<? extends AnyValue>> NUMBER_TYPES =
            selectValueTypes((t) -> NumberValue.class.isAssignableFrom(t.valueClass));
    private static final Set<Class<? extends AnyValue>> ARRAY_TYPES =
            selectValueTypes(t -> t.valueRepresentation.canCreateArrayOfValueGroup());
    private static final Set<Class<? extends AnyValue>> TYPES = selectValueTypes(Predicates.alwaysTrue());

    private static Set<Class<? extends AnyValue>> selectValueTypes(Predicate<ValueType> criteria) {
        return Arrays.stream(ValueType.ALL_TYPES)
                .filter(criteria)
                .map(t -> t.valueClass)
                .collect(Collectors.toSet());
    }

    @BeforeEach
    void setUp() {
        this.randomValues = randomValues();
    }

    abstract RandomValues randomValues();

    @Test
    void nextLongValueUnbounded() {
        checkDistribution(randomValues::nextLongValue);
    }

    @Test
    void nextLongValueBounded() {
        checkDistribution(() -> randomValues.nextLongValue(BOUND));
        checkBounded(() -> randomValues.nextLongValue(BOUND));
    }

    @Test
    void nextLongValueBoundedAndShifted() {
        Set<Value> values = new HashSet<>();
        for (int i = 0; i < ITERATIONS; i++) {
            LongValue value = randomValues.nextLongValue(1337, 1337 + BOUND);
            assertThat(value).isNotNull();
            assertThat(value.compareTo(longValue(1337))).isNotNegative();
            assertThat(value.compareTo(longValue(1337 + BOUND)))
                    .as(value.toString())
                    .isNotPositive();
            values.add(value);
        }

        assertThat(values).hasSizeGreaterThan(1);
    }

    @Test
    void nextBooleanValue() {
        checkDistribution(randomValues::nextBooleanValue);
    }

    @Test
    void nextIntValueUnbounded() {
        checkDistribution(randomValues::nextIntValue);
    }

    @Test
    void nextIntValueBounded() {
        checkDistribution(() -> randomValues.nextIntValue(BOUND));
        checkBounded(() -> randomValues.nextIntValue(BOUND));
    }

    @Test
    void nextShortValueUnbounded() {
        checkDistribution(randomValues::nextShortValue);
    }

    @Test
    void nextShortValueBounded() {
        checkDistribution(() -> randomValues.nextShortValue(BOUND));
        checkBounded(() -> randomValues.nextShortValue(BOUND));
    }

    @Test
    void nextByteValueUnbounded() {
        checkDistribution(randomValues::nextByteValue);
    }

    @Test
    void nextByteValueBounded() {
        checkDistribution(() -> randomValues.nextByteValue(BOUND));
        checkBounded(() -> randomValues.nextByteValue(BOUND));
    }

    @Test
    void nextFloatValue() {
        checkDistribution(randomValues::nextFloatValue);
    }

    @Test
    void nextDoubleValue() {
        checkDistribution(randomValues::nextDoubleValue);
    }

    @Test
    void nextNumberValue() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            var seen = new HashSet<>(NUMBER_TYPES);

            while (!seen.isEmpty()) {
                NumberValue numberValue = randomValues.nextNumberValue();
                assertThat(NUMBER_TYPES).contains(numberValue.getClass());
                seen.remove(numberValue.getClass());
            }
        });
    }

    @Test
    void nextAlphaNumericString() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            Set<Integer> seenDigits = "ABCDEFGHIJKLMNOPQRSTUVXYZabcdefghijklmnopqrstuvxyz0123456789"
                    .chars()
                    .boxed()
                    .collect(Collectors.toSet());
            while (!seenDigits.isEmpty()) {
                {
                    TextValue textValue = randomValues.nextAlphaNumericTextValue(10, 20);
                    String asString = textValue.stringValue();
                    for (int j = 0; j < asString.length(); j++) {
                        int ch = asString.charAt(j);
                        assertThat(isAlphabetic(ch) || isDigit(ch))
                                .as("Not a character nor letter: " + ch)
                                .isTrue();
                        seenDigits.remove(ch);
                    }
                }
            }
        });
    }

    @Test
    void nextAsciiString() {
        for (int i = 0; i < ITERATIONS; i++) {
            TextValue textValue = randomValues.nextAsciiTextValue(10, 20);
            String asString = textValue.stringValue();
            int length = asString.length();
            assertThat(length).isGreaterThanOrEqualTo(10).isLessThanOrEqualTo(20);
        }
    }

    @Test
    void nextString() {
        for (int i = 0; i < ITERATIONS; i++) {
            TextValue textValue = randomValues.nextTextValue(10, 20);
            String asString = textValue.stringValue();
            int length = asString.codePointCount(0, asString.length());
            assertThat(length).isGreaterThanOrEqualTo(10).isLessThanOrEqualTo(20);
        }
    }

    @Test
    void nextArray() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            var seen = new HashSet<>(ARRAY_TYPES);
            while (!seen.isEmpty()) {
                ArrayValue arrayValue = randomValues.nextArray();
                assertThat(arrayValue.intSize()).isPositive();
                AnyValue value = arrayValue.value(0);
                assertKnownType(value.getClass(), TYPES);
                markSeen(value.getClass(), seen);
            }
        });
    }

    @Test
    void nextValue() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            var seen = new HashSet<>(TYPES);

            while (!seen.isEmpty()) {
                Value value = randomValues.nextValue();
                assertKnownType(value.getClass(), TYPES);
                markSeen(value.getClass(), seen);
            }
        });
    }

    @Test
    void nextValueOfTypes() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            ValueType[] allTypes = ValueType.ALL_TYPES;
            ValueType[] including = randomValues.selection(allTypes, 1, allTypes.length, false);
            Set<Class<? extends AnyValue>> seen = new HashSet<>();
            for (ValueType type : including) {
                seen.add(type.valueClass);
            }
            while (!seen.isEmpty()) {
                Value value = randomValues.nextValueOfTypes(including);
                assertValueAmongTypes(including, value);
                markSeen(value.getClass(), seen);
            }
        });
    }

    @Test
    void excluding() {
        ValueType[] allTypes = ValueType.ALL_TYPES;
        ValueType[] excluding = randomValues.selection(allTypes, 1, allTypes.length, false);
        ValueType[] including = RandomValues.excluding(excluding);
        for (ValueType excludedType : excluding) {
            if (ArrayUtils.contains(including, excludedType)) {
                fail("Including array " + Arrays.toString(including) + " contains excluded type " + excludedType);
            }
        }
    }

    @Test
    void nextBasicMultilingualPlaneTextValue() {
        for (int i = 0; i < ITERATIONS; i++) {
            TextValue value = randomValues.nextBasicMultilingualPlaneTextValue();
            // make sure the value fits in 16bits, meaning that the size of the char[]
            // matches the number of code points.
            assertThat(value.length()).isEqualTo(value.stringValue().length());
        }
    }

    private static void assertValueAmongTypes(ValueType[] types, Value value) {
        for (ValueType type : types) {
            if (type.valueClass.isAssignableFrom(value.getClass())) {
                return;
            }
        }
        fail("Value " + value + " was not among types " + Arrays.toString(types));
    }

    private static void assertKnownType(Class<? extends AnyValue> typeToCheck, Set<Class<? extends AnyValue>> types) {
        for (Class<? extends AnyValue> type : types) {
            if (type.isAssignableFrom(typeToCheck)) {
                return;
            }
        }
        fail(typeToCheck + " is not an expected type ");
    }

    private static void markSeen(Class<? extends AnyValue> typeToCheck, Set<Class<? extends AnyValue>> seen) {
        seen.removeIf(t -> t.isAssignableFrom(typeToCheck));
    }

    private static void checkDistribution(Supplier<Value> supplier) {
        Set<Value> values = new HashSet<>();
        for (int i = 0; i < ITERATIONS; i++) {
            Value value = supplier.get();
            assertThat(value).isNotNull();
            values.add(value);
        }

        assertThat(values).size().isGreaterThan(1);
    }

    private static void checkBounded(Supplier<NumberValue> supplier) {
        for (int i = 0; i < ITERATIONS; i++) {
            NumberValue value = supplier.get();
            assertThat(value).isNotNull();
            assertThat(value.compareTo(ZERO_INT)).isNotNegative();
            assertThat(value.compareTo(UPPER)).isNegative();
        }
    }
}

class RandomRandomValuesTest extends RandomValuesTest {
    @Override
    RandomValues randomValues() {
        return RandomValues.create(ThreadLocalRandom.current());
    }
}

class SplittableRandomValuesTest extends RandomValuesTest {
    @Override
    RandomValues randomValues() {
        return RandomValues.create(new SplittableRandom());
    }
}
