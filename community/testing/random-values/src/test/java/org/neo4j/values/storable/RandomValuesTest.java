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
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.values.storable.Values.ZERO_INT;
import static org.neo4j.values.storable.Values.longValue;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.values.AnyValue;

abstract class RandomValuesTest {
    private static final int ITERATIONS = 500;
    private static final Duration TIMEOUT = Duration.ofMinutes(2);

    private RandomValues randomValues;

    private static final byte BOUND = 100;
    private static final LongValue UPPER = longValue(BOUND);
    private static final Set<Class<? extends NumberValue>> NUMBER_TYPES = new HashSet<>(Arrays.asList(
            LongValue.class, IntValue.class, ShortValue.class, ByteValue.class, FloatValue.class, DoubleValue.class));

    private static final Set<Class<? extends AnyValue>> TYPES = new HashSet<>(Arrays.asList(
            LongValue.class,
            IntValue.class,
            ShortValue.class,
            ByteValue.class,
            FloatValue.class,
            DoubleValue.class,
            TextValue.class,
            BooleanValue.class,
            PointValue.class,
            DateTimeValue.class,
            LocalDateTimeValue.class,
            DateValue.class,
            TimeValue.class,
            LocalTimeValue.class,
            DurationValue.class));

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
            assertThat(value.compareTo(longValue(1337))).isGreaterThanOrEqualTo(0);
            assertThat(value.compareTo(longValue(1337 + BOUND)))
                    .as(value.toString())
                    .isLessThanOrEqualTo(0);
            values.add(value);
        }

        assertThat(values.size()).isGreaterThan(1);
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
            Set<Class<? extends NumberValue>> seen = new HashSet<>(NUMBER_TYPES);

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
                        assertTrue(isAlphabetic(ch) || isDigit(ch), "Not a character nor letter: " + ch);
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
            assertThat(length).isGreaterThanOrEqualTo(10);
            assertThat(length).isLessThanOrEqualTo(20);
        }
    }

    @Test
    void nextString() {
        for (int i = 0; i < ITERATIONS; i++) {
            TextValue textValue = randomValues.nextTextValue(10, 20);
            String asString = textValue.stringValue();
            int length = asString.codePointCount(0, asString.length());
            assertThat(length).isGreaterThanOrEqualTo(10);
            assertThat(length).isLessThanOrEqualTo(20);
        }
    }

    @Test
    void nextArray() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            Set<Class<? extends AnyValue>> seen = new HashSet<>(TYPES);
            while (!seen.isEmpty()) {
                ArrayValue arrayValue = randomValues.nextArray();
                assertThat(arrayValue.intSize()).isGreaterThanOrEqualTo(1);
                AnyValue value = arrayValue.value(0);
                assertKnownType(value.getClass(), TYPES);
                markSeen(value.getClass(), seen);
            }
        });
    }

    @Test
    void nextValue() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            Set<Class<? extends AnyValue>> all = new HashSet<>(TYPES);
            all.add(ArrayValue.class);
            Set<Class<? extends AnyValue>> seen = new HashSet<>(all);

            while (!seen.isEmpty()) {
                Value value = randomValues.nextValue();
                assertKnownType(value.getClass(), all);
                markSeen(value.getClass(), seen);
            }
        });
    }

    @Test
    void nextValueOfTypes() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            ValueType[] allTypes = ValueType.values();
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
        ValueType[] allTypes = ValueType.values();
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

        assertThat(values.size()).isGreaterThan(1);
    }

    private static void checkBounded(Supplier<NumberValue> supplier) {
        for (int i = 0; i < ITERATIONS; i++) {
            NumberValue value = supplier.get();
            assertThat(value).isNotNull();
            assertThat(value.compareTo(ZERO_INT)).isGreaterThanOrEqualTo(0);
            assertThat(value.compareTo(UPPER)).isLessThan(0);
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
