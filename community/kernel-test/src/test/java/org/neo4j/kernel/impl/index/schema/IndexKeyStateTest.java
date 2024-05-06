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
package org.neo4j.kernel.impl.index.schema;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.neo4j.kernel.impl.index.schema.NativeIndexKey.Inclusion.NEUTRAL;
import static org.neo4j.kernel.impl.index.schema.NativeIndexKey.NO_ENTITY_ID;
import static org.neo4j.values.storable.ValueGroup.GEOMETRY;
import static org.neo4j.values.storable.ValueGroup.GEOMETRY_ARRAY;
import static org.neo4j.values.storable.ValueGroup.NUMBER;
import static org.neo4j.values.storable.ValueGroup.NUMBER_ARRAY;
import static org.neo4j.values.storable.ValueGroup.TEXT;
import static org.neo4j.values.storable.ValueGroup.TEXT_ARRAY;
import static org.neo4j.values.storable.Values.COMPARATOR;
import static org.neo4j.values.storable.Values.booleanArray;
import static org.neo4j.values.storable.Values.byteArray;
import static org.neo4j.values.storable.Values.dateArray;
import static org.neo4j.values.storable.Values.dateTimeArray;
import static org.neo4j.values.storable.Values.doubleArray;
import static org.neo4j.values.storable.Values.durationArray;
import static org.neo4j.values.storable.Values.floatArray;
import static org.neo4j.values.storable.Values.intArray;
import static org.neo4j.values.storable.Values.isGeometryArray;
import static org.neo4j.values.storable.Values.isGeometryValue;
import static org.neo4j.values.storable.Values.localDateTimeArray;
import static org.neo4j.values.storable.Values.localTimeArray;
import static org.neo4j.values.storable.Values.longArray;
import static org.neo4j.values.storable.Values.of;
import static org.neo4j.values.storable.Values.pointArray;
import static org.neo4j.values.storable.Values.shortArray;
import static org.neo4j.values.storable.Values.timeArray;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.io.pagecache.ByteArrayPageCursor;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.string.UTF8;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.ByteArray;
import org.neo4j.values.storable.ByteValue;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DoubleArray;
import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.FloatArray;
import org.neo4j.values.storable.FloatValue;
import org.neo4j.values.storable.IntArray;
import org.neo4j.values.storable.IntValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.LongArray;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.PointArray;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.ShortArray;
import org.neo4j.values.storable.ShortValue;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.Values;

@ExtendWith(RandomExtension.class)
@TestInstance(PER_CLASS)
abstract class IndexKeyStateTest<KEY extends GenericKey<KEY>> {
    @Inject
    RandomSupport random;

    @BeforeEach
    void setupRandomConfig() {
        random = random.withConfiguration(new RandomValues.Configuration() {
            @Override
            public int stringMinLength() {
                return 0;
            }

            @Override
            public int stringMaxLength() {
                return 50;
            }

            @Override
            public int arrayMinLength() {
                return 0;
            }

            @Override
            public int arrayMaxLength() {
                return 10;
            }

            @Override
            public int maxCodePoint() {
                return RandomValues.MAX_BMP_CODE_POINT;
            }

            @Override
            public int minCodePoint() {
                return Character.MIN_CODE_POINT;
            }
        });
        random.reset();
    }

    @ParameterizedTest
    @MethodSource("validValueGenerators")
    void readWhatIsWritten(ValueGenerator valueGenerator) {
        // Given
        PageCursor cursor = newPageCursor();
        KEY writeState = newKeyState();
        Value value = valueGenerator.next();
        int offset = cursor.getOffset();

        // When
        writeState.writeValue(value, NEUTRAL);
        writeState.put(cursor);

        // Then
        KEY readState = newKeyState();
        int size = writeState.size();
        cursor.setOffset(offset);
        assertTrue(readState.get(cursor, size), "failed to read");
        assertEquals(0, readState.compareValueTo(writeState), "key states are not equal");
        Value readValue = readState.asValue();
        assertEquals(value, readValue, "deserialized values are not equal");
    }

    @ParameterizedTest
    @MethodSource("validValueGenerators")
    void readWhatIsWrittenCompositeKey(ValueGenerator valueGenerator) {
        // Given
        int nbrOfSlots = random.nextInt(2, 5);
        PageCursor cursor = newPageCursor();
        Layout<KEY> layout = newLayout(nbrOfSlots);
        KEY writeState = layout.newKey();
        int offset = cursor.getOffset();

        // When
        Value[] writtenValues = generateValuesForCompositeKey(nbrOfSlots, valueGenerator);

        for (int slot = 0; slot < nbrOfSlots; slot++) {
            writeState.writeValue(slot, writtenValues[slot], NEUTRAL);
        }

        writeState.put(cursor);

        // Then
        KEY readState = layout.newKey();
        int size = writeState.size();
        cursor.setOffset(offset);
        assertTrue(readState.get(cursor, size), "failed to read");
        assertEquals(0, readState.compareValueTo(writeState), "key states are not equal");
        Value[] readValues = readState.asValues();
        assertThat(readValues).isEqualTo(writtenValues);
    }

    @ParameterizedTest
    @MethodSource("validValueGenerators")
    void copyShouldCopy(ValueGenerator valueGenerator) {
        // Given
        KEY from = newKeyState();
        Value value = valueGenerator.next();
        from.writeValue(value, NEUTRAL);
        KEY to = genericKeyStateWithSomePreviousState(valueGenerator);

        // When
        to.copyFrom(from);

        // Then
        assertEquals(0, from.compareValueTo(to), "states not equals after copy");
    }

    @ParameterizedTest
    @MethodSource("validValueGenerators")
    void copyShouldCopyCompositeKey(ValueGenerator valueGenerator) {
        // Given
        int nbrOfSlots = random.nextInt(2, 5);
        Layout<KEY> layout = newLayout(nbrOfSlots);
        KEY from = layout.newKey();
        Value[] values = generateValuesForCompositeKey(nbrOfSlots, valueGenerator);
        for (int slot = 0; slot < nbrOfSlots; slot++) {
            from.writeValue(slot, values[slot], NEUTRAL);
        }

        KEY to = compositeKeyStateWithSomePreviousState(layout, nbrOfSlots, valueGenerator);

        // When
        to.copyFrom(from);

        // Then
        assertEquals(0, from.compareValueTo(to), "states not equals after copy");
    }

    @Test
    void copyShouldCopyExtremeValues() {
        // Given
        KEY extreme = newKeyState();
        KEY copy = newKeyState();

        for (ValueGroup valueGroup : ValueGroup.values()) {
            if (valueGroup != ValueGroup.NO_VALUE) {
                extreme.initValueAsLowest(valueGroup);
                copy.copyFrom(extreme);
                assertEquals(0, extreme.compareValueTo(copy), "states not equals after copy, valueGroup=" + valueGroup);
                extreme.initValueAsHighest(valueGroup);
                copy.copyFrom(extreme);
                assertEquals(0, extreme.compareValueTo(copy), "states not equals after copy, valueGroup=" + valueGroup);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("validComparableValueGenerators")
    void compareToMustAlignWithValuesCompareTo(ValueGenerator valueGenerator) {
        // Given
        List<Value> values = new ArrayList<>();
        List<KEY> states = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Value value = valueGenerator.next();
            values.add(value);
            KEY state = newKeyState();
            state.writeValue(value, NEUTRAL);
            states.add(state);
        }

        // When
        values.sort(COMPARATOR);
        states.sort(GenericKey::compareValueTo);

        // Then
        for (int i = 0; i < values.size(); i++) {
            assertEquals(values.get(i), states.get(i).asValue(), "sort order was different");
        }
    }

    @ParameterizedTest
    @MethodSource("validComparableValueGenerators")
    void compositeKeyCompareToMustAlignWithValuesCompareTo(ValueGenerator valueGenerator) {
        int nbrOfSlots = random.nextInt(2, 5);
        List<KEY> states = new ArrayList<>();
        Layout<KEY> layout = newLayout(nbrOfSlots);
        for (int i = 0; i < 10; i++) {
            KEY key = layout.newKey();
            states.add(key);
            for (int slot = 0; slot < nbrOfSlots; slot++) {
                key.writeValue(slot, valueGenerator.next(), NEUTRAL);
            }
        }

        states.sort(GenericKey::compareValueTo);
        for (int i = 0; i < 9; i++) {
            KEY key1 = states.get(i);
            KEY key2 = states.get(i + 1);

            for (int slot = 0; slot < nbrOfSlots; slot++) {
                var result = COMPARATOR.compare(key1.asValues()[slot], key2.asValues()[slot]);
                if (result < 0) {
                    break;
                }

                if (result > 0) {
                    fail("Keys incorrectly ordered: " + key1 + " , " + key2);
                }
            }
        }
    }

    // The reason this test doesn't test incomparable values is that it relies on ordering being same as that of the
    // Values module.
    @ParameterizedTest
    @MethodSource("validComparableValueGenerators")
    void mustProduceValidMinimalSplitters(ValueGenerator valueGenerator) {
        // Given
        Value value1 = valueGenerator.next();
        Value value2;
        do {
            value2 = valueGenerator.next();
        } while (COMPARATOR.compare(value1, value2) == 0);

        // When
        Value left = pickSmaller(value1, value2);
        Value right = left == value1 ? value2 : value1;

        KEY leftState = newKeyState();
        leftState.writeValue(left, NEUTRAL);
        KEY rightState = newKeyState();
        rightState.writeValue(right, NEUTRAL);

        // Then
        assertValidMinimalSplitter(leftState, rightState, this::newKeyState);
    }

    @ParameterizedTest
    @MethodSource("validComparableValueGenerators")
    void mustProduceValidMinimalSplittersCompositeKey(ValueGenerator valueGenerator) {
        int nbrOfSlots = random.nextInt(2, 5);
        Layout<KEY> layout = newLayout(nbrOfSlots);
        KEY key1 = layout.newKey();
        Value[] values1 = generateValuesForCompositeKey(nbrOfSlots, valueGenerator);
        KEY key2 = layout.newKey();
        Value[] values2;
        do {
            values2 = generateValuesForCompositeKey(nbrOfSlots, valueGenerator);
        } while (Arrays.equals(values1, values2));
        for (int slot = 0; slot < nbrOfSlots; slot++) {
            key1.writeValue(slot, values1[slot], NEUTRAL);
            key2.writeValue(slot, values2[slot], NEUTRAL);
        }

        KEY leftState = key1.compareValueTo(key2) < 0 ? key1 : key2;
        KEY rightState = leftState == key1 ? key2 : key1;

        assertValidMinimalSplitter(leftState, rightState, layout::newKey);
    }

    @ParameterizedTest
    @MethodSource("validValueGenerators")
    void mustProduceValidMinimalSplittersWhenValuesAreEqual(ValueGenerator valueGenerator) {
        Value value = valueGenerator.next();
        KEY leftState = newKeyState();
        leftState.writeValue(value, NEUTRAL);
        KEY rightState = newKeyState();
        rightState.writeValue(value, NEUTRAL);

        assertValidMinimalSplitterForEqualValues(leftState, rightState, this::newKeyState);
    }

    @ParameterizedTest
    @MethodSource("validValueGenerators")
    void mustProduceValidMinimalSplittersWhenValuesAreEqualCompositeKey(ValueGenerator valueGenerator) {
        int nbrOfSlots = random.nextInt(2, 5);
        Layout<KEY> layout = newLayout(nbrOfSlots);
        KEY leftState = layout.newKey();
        KEY rightState = layout.newKey();
        Value[] values = generateValuesForCompositeKey(nbrOfSlots, valueGenerator);

        for (int slot = 0; slot < nbrOfSlots; slot++) {
            leftState.writeValue(slot, values[slot], NEUTRAL);
            rightState.writeValue(slot, values[slot], NEUTRAL);
        }

        assertValidMinimalSplitterForEqualValues(leftState, rightState, layout::newKey);
    }

    @ParameterizedTest
    @MethodSource("validValueGenerators")
    void mustReportCorrectSize(ValueGenerator valueGenerator) {
        // Given
        PageCursor cursor = newPageCursor();
        Value value = valueGenerator.next();
        KEY state = newKeyState();
        state.writeValue(value, NEUTRAL);
        int offsetBefore = cursor.getOffset();

        // When
        int reportedSize = state.size();
        state.put(cursor);
        int offsetAfter = cursor.getOffset();

        // Then
        int actualSize = offsetAfter - offsetBefore;
        assertEquals(
                reportedSize,
                actualSize,
                String.format(
                        "did not report correct size, value=%s, actualSize=%d, reportedSize=%d",
                        value, actualSize, reportedSize));
    }

    @ParameterizedTest
    @MethodSource("validValueGenerators")
    void mustReportCorrectSizeCompositeKey(ValueGenerator valueGenerator) {
        // Given
        int nbrOfSlots = random.nextInt(2, 5);
        PageCursor cursor = newPageCursor();
        Layout<KEY> layout = newLayout(nbrOfSlots);
        KEY state = layout.newKey();

        Value[] writtenValues = generateValuesForCompositeKey(nbrOfSlots, valueGenerator);

        for (int slot = 0; slot < nbrOfSlots; slot++) {
            state.writeValue(slot, writtenValues[slot], NEUTRAL);
        }

        int offsetBefore = cursor.getOffset();

        // When
        int reportedSize = state.size();
        state.put(cursor);
        int offsetAfter = cursor.getOffset();

        // Then
        int actualSize = offsetAfter - offsetBefore;
        assertEquals(
                reportedSize,
                actualSize,
                String.format(
                        "did not report correct size, value=%s, actualSize=%d, reportedSize=%d",
                        Arrays.toString(writtenValues), actualSize, reportedSize));
    }

    @Test
    void lowestMustBeLowest() {
        // GEOMETRY
        assertLowest(PointValue.MIN_VALUE);
        // ZONED_DATE_TIME
        assertLowest(DateTimeValue.MIN_VALUE);
        // LOCAL_DATE_TIME
        assertLowest(LocalDateTimeValue.MIN_VALUE);
        // DATE
        assertLowest(DateValue.MIN_VALUE);
        // ZONED_TIME
        assertLowest(TimeValue.MIN_VALUE);
        // LOCAL_TIME
        assertLowest(LocalTimeValue.MIN_VALUE);
        // DURATION (duration, period)
        assertLowest(DurationValue.duration(Duration.ofSeconds(Long.MIN_VALUE, 0)));
        assertLowest(DurationValue.duration(Period.of(Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE)));
        // TEXT
        assertLowest(of(UTF8.decode(new byte[0])));
        // BOOLEAN
        assertLowest(of(false));
        // NUMBER (byte, short, int, long, float, double)
        assertLowest(of(Byte.MIN_VALUE));
        assertLowest(of(Short.MIN_VALUE));
        assertLowest(of(Integer.MIN_VALUE));
        assertLowest(of(Long.MIN_VALUE));
        assertLowest(of(Float.NEGATIVE_INFINITY));
        assertLowest(of(Double.NEGATIVE_INFINITY));
        // GEOMETRY_ARRAY
        assertLowest(pointArray(new PointValue[0]));
        // ZONED_DATE_TIME_ARRAY
        assertLowest(dateTimeArray(new ZonedDateTime[0]));
        // LOCAL_DATE_TIME_ARRAY
        assertLowest(localDateTimeArray(new LocalDateTime[0]));
        // DATE_ARRAY
        assertLowest(dateArray(new LocalDate[0]));
        // ZONED_TIME_ARRAY
        assertLowest(timeArray(new OffsetTime[0]));
        // LOCAL_TIME_ARRAY
        assertLowest(localTimeArray(new LocalTime[0]));
        // DURATION_ARRAY (DurationValue, TemporalAmount)
        assertLowest(durationArray(new DurationValue[0]));
        assertLowest(durationArray(new TemporalAmount[0]));
        // TEXT_ARRAY
        assertLowest(of(ArrayUtils.EMPTY_STRING_ARRAY));
        // BOOLEAN_ARRAY
        assertLowest(of(ArrayUtils.EMPTY_BOOLEAN_ARRAY));
        // NUMBER_ARRAY (byte[], short[], int[], long[], float[], double[])
        assertLowest(of(ArrayUtils.EMPTY_BYTE_ARRAY));
        assertLowest(of(ArrayUtils.EMPTY_SHORT_ARRAY));
        assertLowest(of(ArrayUtils.EMPTY_INT_ARRAY));
        assertLowest(of(ArrayUtils.EMPTY_LONG_ARRAY));
        assertLowest(of(ArrayUtils.EMPTY_FLOAT_ARRAY));
        assertLowest(of(ArrayUtils.EMPTY_DOUBLE_ARRAY));
    }

    @Test
    void highestMustBeHighest() {
        // GEOMETRY
        assertHighest(PointValue.MAX_VALUE);
        // ZONED_DATE_TIME
        assertHighest(DateTimeValue.MAX_VALUE);
        // LOCAL_DATE_TIME
        assertHighest(LocalDateTimeValue.MAX_VALUE);
        // DATE
        assertHighest(DateValue.MAX_VALUE);
        // ZONED_TIME
        assertHighest(TimeValue.MAX_VALUE);
        // LOCAL_TIME
        assertHighest(LocalTimeValue.MAX_VALUE);
        // DURATION (duration, period)
        assertHighest(DurationValue.duration(Duration.ofSeconds(Long.MAX_VALUE, 999_999_999)));
        assertHighest(DurationValue.duration(Period.of(Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE)));
        // TEXT
        assertHighestString();
        // BOOLEAN
        assertHighest(of(true));
        // NUMBER (byte, short, int, long, float, double)
        assertHighest(of(Byte.MAX_VALUE));
        assertHighest(of(Short.MAX_VALUE));
        assertHighest(of(Integer.MAX_VALUE));
        assertHighest(of(Long.MAX_VALUE));
        assertHighest(of(Float.POSITIVE_INFINITY));
        assertHighest(of(Double.POSITIVE_INFINITY));
        // GEOMETRY_ARRAY
        assertHighest(pointArray(new PointValue[] {PointValue.MAX_VALUE}));
        // ZONED_DATE_TIME_ARRAY
        assertHighest(dateTimeArray(new ZonedDateTime[] {DateTimeValue.MAX_VALUE.asObjectCopy()}));
        // LOCAL_DATE_TIME_ARRAY
        assertHighest(localDateTimeArray(new LocalDateTime[] {LocalDateTimeValue.MAX_VALUE.asObjectCopy()}));
        // DATE_ARRAY
        assertHighest(dateArray(new LocalDate[] {DateValue.MAX_VALUE.asObjectCopy()}));
        // ZONED_TIME_ARRAY
        assertHighest(timeArray(new OffsetTime[] {TimeValue.MAX_VALUE.asObjectCopy()}));
        // LOCAL_TIME_ARRAY
        assertHighest(localTimeArray(new LocalTime[] {LocalTimeValue.MAX_VALUE.asObjectCopy()}));
        // DURATION_ARRAY (DurationValue, TemporalAmount)
        assertHighest(durationArray(
                new DurationValue[] {DurationValue.duration(Duration.ofSeconds(Long.MAX_VALUE, 999_999_999))}));
        assertHighest(durationArray(new DurationValue[] {
            DurationValue.duration(Period.of(Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE))
        }));
        assertHighest(durationArray(new TemporalAmount[] {Duration.ofSeconds(Long.MAX_VALUE, 999_999_999)}));
        assertHighest(durationArray(
                new TemporalAmount[] {Period.of(Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE)}));
        // TEXT_ARRAY
        assertHighestStringArray();
        // BOOLEAN_ARRAY
        assertHighest(booleanArray(new boolean[] {true}));
        // NUMBER_ARRAY (byte[], short[], int[], long[], float[], double[])
        assertHighest(byteArray(new byte[] {Byte.MAX_VALUE}));
        assertHighest(shortArray(new short[] {Short.MAX_VALUE}));
        assertHighest(intArray(new int[] {Integer.MAX_VALUE}));
        assertHighest(longArray(new long[] {Long.MAX_VALUE}));
        assertHighest(floatArray(new float[] {Float.POSITIVE_INFINITY}));
        assertHighest(doubleArray(new double[] {Double.POSITIVE_INFINITY}));
    }

    @Test
    void shouldNeverOverwriteDereferencedTextValues() {
        // Given a value that we dereference
        Value srcValue = Values.utf8Value("First string".getBytes(UTF_8));
        KEY genericKeyState = newKeyState();
        genericKeyState.writeValue(srcValue, NEUTRAL);
        Value dereferencedValue = genericKeyState.asValue();
        assertEquals(srcValue, dereferencedValue);

        // and write to page
        PageCursor cursor = newPageCursor();
        int offset = cursor.getOffset();
        genericKeyState.put(cursor);
        int keySize = cursor.getOffset() - offset;
        cursor.setOffset(offset);

        // we should not overwrite the first dereferenced value when initializing from a new value
        genericKeyState.clear();
        Value srcValue2 = Values.utf8Value("Secondstring".getBytes(UTF_8)); // <- Same length as first string
        genericKeyState.writeValue(srcValue2, NEUTRAL);
        Value dereferencedValue2 = genericKeyState.asValue();
        assertEquals(srcValue2, dereferencedValue2);
        assertEquals(srcValue, dereferencedValue);

        // and we should not overwrite the second value when we read back the first value from page
        genericKeyState.clear();
        genericKeyState.get(cursor, keySize);
        Value dereferencedValue3 = genericKeyState.asValue();
        assertEquals(srcValue, dereferencedValue3);
        assertEquals(srcValue2, dereferencedValue2);
        assertEquals(srcValue, dereferencedValue);
    }

    @Test
    void indexedCharShouldComeBackAsCharValue() {
        shouldReadBackToExactOriginalValue(random.randomValues().nextCharValue());
    }

    @Test
    void indexedCharArrayShouldComeBackAsCharArrayValue() {
        shouldReadBackToExactOriginalValue(random.randomValues().nextCharArray());
    }

    /* TESTS FOR KEY STATE (including entityId) */

    @ParameterizedTest
    @MethodSource("validValueGenerators")
    void minimalSplitterForSameValueShouldDivideLeftAndRight(ValueGenerator valueGenerator) {
        // Given
        Value value = valueGenerator.next();
        Layout<KEY> layout = newLayout(1);
        KEY left = layout.newKey();
        KEY right = layout.newKey();
        KEY minimalSplitter = layout.newKey();

        // keys with same value but different entityId
        left.initialize(1);
        left.initFromValue(0, value, NEUTRAL);
        right.initialize(2);
        right.initFromValue(0, value, NEUTRAL);

        // When creating minimal splitter
        layout.minimalSplitter(left, right, minimalSplitter);

        // Then that minimal splitter need to correctly divide left and right
        assertTrue(
                layout.compare(left, minimalSplitter) < 0,
                "Expected minimal splitter to be strictly greater than left but wasn't for value " + value);
        assertTrue(
                layout.compare(minimalSplitter, right) <= 0,
                "Expected right to be greater than or equal to minimal splitter but wasn't for value " + value);
    }

    @ParameterizedTest
    @MethodSource("validValueGenerators")
    void minimalSplitterShouldRemoveEntityIdIfPossible(ValueGenerator valueGenerator) {
        // Given
        Value firstValue = valueGenerator.next();
        Value secondValue = uniqueSecondValue(valueGenerator, firstValue);
        Value leftValue = pickSmaller(firstValue, secondValue);
        Value rightValue = pickOther(firstValue, secondValue, leftValue);

        Layout<KEY> layout = newLayout(1);
        KEY left = layout.newKey();
        KEY right = layout.newKey();
        KEY minimalSplitter = layout.newKey();

        // keys with unique values
        left.initialize(1);
        left.initFromValue(0, leftValue, NEUTRAL);
        right.initialize(2);
        right.initFromValue(0, rightValue, NEUTRAL);

        // When creating minimal splitter
        layout.minimalSplitter(left, right, minimalSplitter);

        // Then that minimal splitter should have entity id shaved off
        assertEquals(
                NO_ENTITY_ID,
                minimalSplitter.getEntityId(),
                "Expected minimal splitter to have entityId removed when constructed from keys with unique values: "
                        + "left=" + leftValue + ", right=" + rightValue);
    }

    @ParameterizedTest
    @MethodSource("validValueGenerators")
    void minimalSplitterForSameValueShouldDivideLeftAndRightCompositeKey(ValueGenerator valueGenerator) {
        // Given composite keys with same set of values
        int nbrOfSlots = random.nextInt(2, 5);
        Layout<KEY> layout = newLayout(nbrOfSlots);
        KEY left = layout.newKey();
        KEY right = layout.newKey();
        KEY minimalSplitter = layout.newKey();
        left.initialize(1);
        right.initialize(2);
        Value[] values = new Value[nbrOfSlots];
        for (int slot = 0; slot < nbrOfSlots; slot++) {
            Value value = valueGenerator.next();
            values[slot] = value;
            left.initFromValue(slot, value, NEUTRAL);
            right.initFromValue(slot, value, NEUTRAL);
        }

        // When creating minimal splitter
        layout.minimalSplitter(left, right, minimalSplitter);

        // Then that minimal splitter need to correctly divide left and right
        assertTrue(
                layout.compare(left, minimalSplitter) < 0,
                "Expected minimal splitter to be strictly greater than left but wasn't for value "
                        + Arrays.toString(values));
        assertTrue(
                layout.compare(minimalSplitter, right) <= 0,
                "Expected right to be greater than or equal to minimal splitter but wasn't for value "
                        + Arrays.toString(values));
    }

    @ParameterizedTest
    @MethodSource("validValueGenerators")
    void minimalSplitterShouldRemoveEntityIdIfPossibleCompositeKey(ValueGenerator valueGenerator) {
        // Given
        int nbrOfSlots = random.nextInt(2, 5);
        int differingSlot = random.nextInt(nbrOfSlots);
        Layout<KEY> layout = newLayout(nbrOfSlots);
        KEY left = layout.newKey();
        KEY right = layout.newKey();
        KEY minimalSplitter = layout.newKey();
        left.initialize(1);
        right.initialize(2);
        // Same value on all except one slot
        for (int slot = 0; slot < nbrOfSlots; slot++) {
            if (slot == differingSlot) {
                continue;
            }
            Value value = valueGenerator.next();
            left.initFromValue(slot, value, NEUTRAL);
            right.initFromValue(slot, value, NEUTRAL);
        }
        Value firstValue = valueGenerator.next();
        Value secondValue = uniqueSecondValue(valueGenerator, firstValue);
        Value leftValue = pickSmaller(firstValue, secondValue);
        Value rightValue = pickOther(firstValue, secondValue, leftValue);
        left.initFromValue(differingSlot, leftValue, NEUTRAL);
        right.initFromValue(differingSlot, rightValue, NEUTRAL);

        // When creating minimal splitter
        layout.minimalSplitter(left, right, minimalSplitter);

        // Then that minimal splitter should have entity id shaved off
        assertEquals(
                NO_ENTITY_ID,
                minimalSplitter.getEntityId(),
                "Expected minimal splitter to have entityId removed when constructed from keys with unique values: "
                        + "left=" + leftValue + ", right=" + rightValue);
    }

    /**
     * If this test fails because size of index key has changed, documentation needs to be updated accordingly.
     */
    @ParameterizedTest
    @MethodSource("singleValueGeneratorsStream")
    void testDocumentedKeySizesNonArrays(ValueGenerator generator) {
        Value value = generator.next();
        KEY key = newKeyState();
        key.initFromValue(0, value, NEUTRAL);
        int keySize = key.size();
        int keyOverhead = NativeIndexKey.ENTITY_ID_SIZE;
        int actualSizeOfData = keySize - keyOverhead;

        int expectedSizeOfData;
        String typeName = value.getTypeName();
        expectedSizeOfData = switch (value.valueGroup()) {
            case NUMBER -> getNumberSize(value);
            case BOOLEAN -> 2;
            case DATE ->
            // typeName: Date
            9;
            case ZONED_TIME ->
            // typeName: Time
            13;
            case LOCAL_TIME ->
            // typeName: LocalTime
            9;
            case ZONED_DATE_TIME ->
            // typeName: DateTime
            17;
            case LOCAL_DATE_TIME ->
            // typeName: LocalDateTime
            13;
            case DURATION ->
            // typeName: Duration or Period
            29;
            case GEOMETRY -> getGeometrySize(value);
            case TEXT -> getStringSize(value);
            default -> throw new RuntimeException(
                    "Did not expect this type to be tested in this test. Value was " + value);
        };
        assertKeySize(expectedSizeOfData, actualSizeOfData, typeName);
    }

    /**
     * If this test fails because size of index key has changed, documentation needs to be updated accordingly.
     */
    @SuppressWarnings("DuplicateBranchesInSwitch")
    @ParameterizedTest
    @MethodSource("arrayValueGeneratorsStream")
    void testDocumentedKeySizesArrays(ValueGenerator generator) {
        Value value = generator.next();
        KEY key = newKeyState();
        key.initFromValue(0, value, NEUTRAL);
        int keySize = key.size();
        int keyOverhead = NativeIndexKey.ENTITY_ID_SIZE;
        int actualSizeOfData = keySize - keyOverhead;

        int arrayLength = 0;
        if (value instanceof ArrayValue) {
            arrayLength = ((ArrayValue) value).length();
        }

        int normalArrayOverhead = 3;
        int numberArrayOverhead = 4;
        int geometryArrayOverhead = 6;

        int arrayOverhead;
        int arrayElementSize;
        String typeName = value.getTypeName();
        switch (value.valueGroup()) {
            case NUMBER_ARRAY -> {
                arrayOverhead = numberArrayOverhead;
                arrayElementSize = getNumberArrayElementSize(value);
            }
            case BOOLEAN_ARRAY -> {
                arrayOverhead = normalArrayOverhead;
                arrayElementSize = 1;
            }
            case DATE_ARRAY -> {
                // typeName: Date
                arrayOverhead = normalArrayOverhead;
                arrayElementSize = 8;
            }
            case ZONED_TIME_ARRAY -> {
                // typeName: Time
                arrayOverhead = normalArrayOverhead;
                arrayElementSize = 12;
            }
            case LOCAL_TIME_ARRAY -> {
                // typeName: LocalTime
                arrayOverhead = normalArrayOverhead;
                arrayElementSize = 8;
            }
            case ZONED_DATE_TIME_ARRAY -> {
                // typeName: DateTime
                arrayOverhead = normalArrayOverhead;
                arrayElementSize = 16;
            }
            case LOCAL_DATE_TIME_ARRAY -> {
                // typeName: LocalDateTime
                arrayOverhead = normalArrayOverhead;
                arrayElementSize = 12;
            }
            case DURATION_ARRAY -> {
                // typeName: Duration or Period
                arrayOverhead = normalArrayOverhead;
                arrayElementSize = 28;
            }
            case GEOMETRY_ARRAY -> {
                arrayOverhead = geometryArrayOverhead;
                arrayElementSize = getGeometryArrayElementSize(value, arrayLength);
            }
            case TEXT_ARRAY -> {
                assertTextArraySize(value, actualSizeOfData, normalArrayOverhead, typeName);
                return;
            }
            default -> throw new RuntimeException("Did not expect this type to be tested in this test. Value was "
                    + value + " is value group " + value.valueGroup());
        }
        int expectedSizeOfData = arrayOverhead + arrayLength * arrayElementSize;
        assertKeySize(expectedSizeOfData, actualSizeOfData, typeName);
    }

    private static void assertKeySize(int expectedKeySize, int actualKeySize, String type) {
        assertEquals(
                expectedKeySize,
                actualKeySize,
                "Expected keySize for type " + type + " to be " + expectedKeySize + " but was " + actualKeySize);
    }

    private void shouldReadBackToExactOriginalValue(Value srcValue) {
        // given
        KEY state = newKeyState();
        state.clear();
        state.writeValue(srcValue, NEUTRAL);
        Value retrievedValueAfterWrittenToState = state.asValue();
        assertEquals(srcValue, retrievedValueAfterWrittenToState);
        assertEquals(srcValue.getClass(), retrievedValueAfterWrittenToState.getClass());

        // ... which is written to cursor
        PageCursor cursor = newPageCursor();
        int offset = cursor.getOffset();
        state.put(cursor);
        int keySize = cursor.getOffset() - offset;
        cursor.setOffset(offset);

        // when reading it back
        state.clear();
        state.get(cursor, keySize);

        // then it should also be retrieved as char value
        Value retrievedValueAfterReadFromCursor = state.asValue();
        assertEquals(srcValue, retrievedValueAfterReadFromCursor);
        assertEquals(srcValue.getClass(), retrievedValueAfterReadFromCursor.getClass());
    }

    private void assertHighestStringArray() {
        for (int i = 0; i < 1000; i++) {
            assertHighest(random.randomValues().nextTextArray());
        }
    }

    private void assertHighestString() {
        for (int i = 0; i < 1000; i++) {
            assertHighest(random.randomValues().nextTextValue());
        }
    }

    private void assertHighest(Value value) {
        KEY highestOfAll = newKeyState();
        KEY highestInValueGroup = newKeyState();
        KEY other = newKeyState();
        highestOfAll.initValueAsHighest(ValueGroup.UNKNOWN);
        highestInValueGroup.initValueAsHighest(value.valueGroup());
        other.writeValue(value, NEUTRAL);
        assertTrue(highestInValueGroup.compareValueTo(other) > 0, "highestInValueGroup not higher than " + value);
        assertTrue(highestOfAll.compareValueTo(other) > 0, "highestOfAll not higher than " + value);
        assertTrue(
                highestOfAll.compareValueTo(highestInValueGroup) > 0 || highestOfAll.type == highestInValueGroup.type,
                "highestOfAll not higher than highestInValueGroup");
    }

    private void assertLowest(Value value) {
        KEY lowestOfAll = newKeyState();
        KEY lowestInValueGroup = newKeyState();
        KEY other = newKeyState();
        lowestOfAll.initValueAsLowest(ValueGroup.UNKNOWN);
        lowestInValueGroup.initValueAsLowest(value.valueGroup());
        other.writeValue(value, NEUTRAL);
        assertTrue(lowestInValueGroup.compareValueTo(other) <= 0);
        assertTrue(lowestOfAll.compareValueTo(other) <= 0);
        assertTrue(lowestOfAll.compareValueTo(lowestInValueGroup) <= 0);
    }

    private static Value pickSmaller(Value value1, Value value2) {
        return COMPARATOR.compare(value1, value2) < 0 ? value1 : value2;
    }

    private void assertValidMinimalSplitter(KEY leftState, KEY rightState, Supplier<KEY> keyFactory) {
        KEY minimalSplitter = keyFactory.get();
        rightState.minimalSplitter(leftState, rightState, minimalSplitter);

        assertTrue(
                leftState.compareValueTo(minimalSplitter) < 0,
                "left state not less than minimal splitter, leftState=" + leftState + ", rightState=" + rightState
                        + ", minimalSplitter=" + minimalSplitter);
        assertTrue(
                rightState.compareValueTo(minimalSplitter) >= 0,
                "right state not greater than or equal to minimal splitter, leftState=" + leftState + ", rightState="
                        + rightState + ", minimalSplitter=" + minimalSplitter);
    }

    private void assertValidMinimalSplitterForEqualValues(KEY leftState, KEY rightState, Supplier<KEY> keyFactory) {
        KEY minimalSplitter = keyFactory.get();
        rightState.minimalSplitter(leftState, rightState, minimalSplitter);

        assertEquals(
                0,
                leftState.compareValueTo(minimalSplitter),
                "left state not equal to minimal splitter, leftState=" + leftState + ", rightState=" + rightState
                        + ", minimalSplitter=" + minimalSplitter);
        assertEquals(
                0,
                rightState.compareValueTo(minimalSplitter),
                "right state not equal to minimal splitter, leftState=" + leftState + ", rightState=" + rightState
                        + ", minimalSplitter=" + minimalSplitter);
    }

    private Value nextValidValue(boolean includeIncomparable) {
        Value value;
        do {
            value = random.randomValues().nextValue();
        } while (!includeIncomparable && isIncomparable(value));
        return value;
    }

    private static boolean isIncomparable(Value value) {
        return isGeometryValue(value) || isGeometryArray(value);
    }

    private ValueGenerator[] listValueGenerators(boolean includeIncomparable) {
        List<ValueGenerator> generators = new ArrayList<>();
        // single
        generators.addAll(singleValueGenerators(includeIncomparable));
        // array
        generators.addAll(arrayValueGenerators(includeIncomparable));
        // and a random
        generators.add(() -> nextValidValue(includeIncomparable));
        return generators.toArray(new ValueGenerator[0]);
    }

    private List<ValueGenerator> singleValueGenerators(boolean includeIncomparable) {
        List<ValueGenerator> generators = new ArrayList<>(asList(
                () -> random.randomValues().nextDateTimeValue(),
                () -> random.randomValues().nextLocalDateTimeValue(),
                () -> random.randomValues().nextDateValue(),
                () -> random.randomValues().nextTimeValue(),
                () -> random.randomValues().nextLocalTimeValue(),
                () -> random.randomValues().nextPeriod(),
                () -> random.randomValues().nextDuration(),
                () -> random.randomValues().nextCharValue(),
                () -> random.randomValues().nextTextValue(),
                () -> random.randomValues().nextAlphaNumericTextValue(),
                () -> random.randomValues().nextBooleanValue(),
                () -> random.randomValues().nextNumberValue()));

        if (includeIncomparable) {
            generators.addAll(asList(
                    () -> random.randomValues().nextPointValue(),
                    () -> random.randomValues().nextGeographicPoint(),
                    () -> random.randomValues().nextGeographic3DPoint(),
                    () -> random.randomValues().nextCartesianPoint(),
                    () -> random.randomValues().nextCartesian3DPoint()));
        }

        return generators;
    }

    private List<ValueGenerator> arrayValueGenerators(boolean includeIncomparable) {
        List<ValueGenerator> generators = new ArrayList<>(asList(
                () -> random.randomValues().nextDateTimeArray(),
                () -> random.randomValues().nextLocalDateTimeArray(),
                () -> random.randomValues().nextDateArray(),
                () -> random.randomValues().nextTimeArray(),
                () -> random.randomValues().nextLocalTimeArray(),
                () -> random.randomValues().nextDurationArray(),
                () -> random.randomValues().nextDurationArray(),
                () -> random.randomValues().nextCharArray(),
                () -> random.randomValues().nextTextArray(),
                () -> random.randomValues().nextAlphaNumericTextArray(),
                () -> random.randomValues().nextBooleanArray(),
                () -> random.randomValues().nextByteArray(),
                () -> random.randomValues().nextShortArray(),
                () -> random.randomValues().nextIntArray(),
                () -> random.randomValues().nextLongArray(),
                () -> random.randomValues().nextFloatArray(),
                () -> random.randomValues().nextDoubleArray()));

        if (includeIncomparable) {
            generators.addAll(asList(
                    () -> random.randomValues().nextPointArray(),
                    () -> random.randomValues().nextGeographicPointArray(),
                    () -> random.randomValues().nextGeographic3DPointArray(),
                    () -> random.randomValues().nextCartesianPointArray(),
                    () -> random.randomValues().nextCartesian3DPointArray()));
        }
        return generators;
    }

    private Stream<ValueGenerator> validValueGenerators() {
        return Stream.of(listValueGenerators(true));
    }

    private Stream<ValueGenerator> singleValueGeneratorsStream() {
        return singleValueGenerators(true).stream();
    }

    private Stream<ValueGenerator> arrayValueGeneratorsStream() {
        return arrayValueGenerators(true).stream();
    }

    private Stream<ValueGenerator> validComparableValueGenerators() {
        return Stream.of(listValueGenerators(includePointTypesForComparisons()));
    }

    private ValueGenerator randomValueGenerator() {
        ValueGenerator[] generators = listValueGenerators(true);
        return generators[random.nextInt(generators.length)];
    }

    // In order to keep the number of combinations in parametrised tests reasonable,
    // the value in the first slot will be taken from the supplied generator.
    // The supplied generator is typically a test parameter and will produce values only of one type.
    // Types for other slots are selected at random, which ensures that this method produces
    // composite keys of various type combinations.
    // At the same time, with a typical usage, each type is guaranteed to be tested
    // at least in the first slot.
    private Value[] generateValuesForCompositeKey(int nbrOfSlots, ValueGenerator firstSlotValueGenerator) {
        Value[] values = new Value[nbrOfSlots];
        values[0] = firstSlotValueGenerator.next();

        for (int slot = 1; slot < nbrOfSlots; slot++) {
            // get a random value of a random type
            values[slot] = randomValueGenerator().next();
        }

        return values;
    }

    private static int getStringSize(Value value) {
        int expectedSizeOfData;
        if (value instanceof TextValue) {
            expectedSizeOfData = 3 + ((TextValue) value).stringValue().getBytes(UTF_8).length;
        } else {
            throw new RuntimeException(
                    "Unexpected class for value in value group " + TEXT + ", was " + value.getClass());
        }
        return expectedSizeOfData;
    }

    private int getGeometrySize(Value value) {
        int dimensions;
        if (value instanceof PointValue) {
            dimensions = ((PointValue) value).coordinate().length;
        } else {
            throw new RuntimeException(
                    "Unexpected class for value in value group " + GEOMETRY + ", was " + value.getClass());
        }
        return getPointSerialisedSize(dimensions);
    }

    private static int getNumberSize(Value value) {
        int expectedSizeOfData;
        if (value instanceof ByteValue) {
            expectedSizeOfData = 3;
        } else if (value instanceof ShortValue) {
            expectedSizeOfData = 4;
        } else if (value instanceof IntValue) {
            expectedSizeOfData = 6;
        } else if (value instanceof LongValue) {
            expectedSizeOfData = 10;
        } else if (value instanceof FloatValue) {
            expectedSizeOfData = 6;
        } else if (value instanceof DoubleValue) {
            expectedSizeOfData = 10;
        } else {
            throw new RuntimeException(
                    "Unexpected class for value in value group " + NUMBER + ", was " + value.getClass());
        }
        return expectedSizeOfData;
    }

    private static int getNumberArrayElementSize(Value value) {
        int arrayElementSize;
        if (value instanceof ByteArray) {
            arrayElementSize = 1;
        } else if (value instanceof ShortArray) {
            arrayElementSize = 2;
        } else if (value instanceof IntArray) {
            arrayElementSize = 4;
        } else if (value instanceof LongArray) {
            arrayElementSize = 8;
        } else if (value instanceof FloatArray) {
            arrayElementSize = 4;
        } else if (value instanceof DoubleArray) {
            arrayElementSize = 8;
        } else {
            throw new RuntimeException(
                    "Unexpected class for value in value group " + NUMBER_ARRAY + ", was " + value.getClass());
        }
        return arrayElementSize;
    }

    private static void assertTextArraySize(
            Value value, int actualSizeOfData, int normalArrayOverhead, String typeName) {
        if (value instanceof TextArray stringArray) {
            int sumOfStrings = 0;
            for (int i = 0; i < stringArray.length(); i++) {
                String string = stringArray.stringValue(i);
                sumOfStrings += 2 + string.getBytes(UTF_8).length;
            }
            int totalTextArraySize = normalArrayOverhead + sumOfStrings;
            assertKeySize(totalTextArraySize, actualSizeOfData, typeName);
        } else {
            throw new RuntimeException(
                    "Unexpected class for value in value group " + TEXT_ARRAY + ", was " + value.getClass());
        }
    }

    private int getGeometryArrayElementSize(Value value, int arrayLength) {
        if (arrayLength < 1) {
            return 0;
        }
        int dimensions;
        if (value instanceof PointArray) {
            dimensions = ((PointArray) value).pointValue(0).coordinate().length;
        } else {
            throw new RuntimeException(
                    "Unexpected class for value in value group " + GEOMETRY_ARRAY + ", was " + value.getClass());
        }
        return getArrayPointSerialisedSize(dimensions);
    }

    private KEY genericKeyStateWithSomePreviousState(ValueGenerator valueGenerator) {
        KEY to = newKeyState();
        if (random.nextBoolean()) {
            // Previous value
            NativeIndexKey.Inclusion inclusion = random.among(NativeIndexKey.Inclusion.values());
            Value value = valueGenerator.next();
            to.writeValue(value, inclusion);
        }
        // No previous state
        return to;
    }

    private KEY compositeKeyStateWithSomePreviousState(
            Layout<KEY> layout, int nbrOfSlots, ValueGenerator valueGenerator) {
        KEY to = layout.newKey();
        if (random.nextBoolean()) {
            Value[] previousValues = generateValuesForCompositeKey(nbrOfSlots, valueGenerator);
            for (int slot = 0; slot < nbrOfSlots; slot++) {
                NativeIndexKey.Inclusion inclusion = random.among(NativeIndexKey.Inclusion.values());
                to.writeValue(slot, previousValues[slot], inclusion);
            }
        }
        // No previous state
        return to;
    }

    private static PageCursor newPageCursor() {
        return ByteArrayPageCursor.wrap(PageCache.PAGE_SIZE);
    }

    private static Value pickOther(Value value1, Value value2, Value currentValue) {
        return currentValue == value1 ? value2 : value1;
    }

    private static Value uniqueSecondValue(ValueGenerator valueGenerator, Value firstValue) {
        Value secondValue;
        do {
            secondValue = valueGenerator.next();
        } while (COMPARATOR.compare(firstValue, secondValue) == 0);
        return secondValue;
    }

    KEY newKeyState() {
        return newLayout(1).newKey();
    }

    abstract Layout<KEY> newLayout(int numberOfSlots);

    abstract boolean includePointTypesForComparisons();

    abstract int getPointSerialisedSize(int dimensions);

    abstract int getArrayPointSerialisedSize(int dimensions);

    @FunctionalInterface
    private interface ValueGenerator {
        Value next();
    }

    interface Layout<KEY extends GenericKey<KEY>> {

        KEY newKey();

        void minimalSplitter(KEY left, KEY right, KEY into);

        int compare(KEY k1, KEY k2);
    }
}
