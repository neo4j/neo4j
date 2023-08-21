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
package org.neo4j.kernel.api.index;

import static java.time.ZoneOffset.UTC;
import static java.util.Arrays.asList;
import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.allEntries;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.boundingBox;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.exact;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.exists;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.range;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.stringContains;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.stringPrefix;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.stringSuffix;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.kernel.api.index.IndexQueryHelper.add;
import static org.neo4j.values.storable.DateTimeValue.datetime;
import static org.neo4j.values.storable.DateValue.epochDate;
import static org.neo4j.values.storable.LocalDateTimeValue.localDateTime;
import static org.neo4j.values.storable.LocalTimeValue.localTime;
import static org.neo4j.values.storable.TimeValue.time;
import static org.neo4j.values.storable.Values.stringValue;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.neo4j.annotations.documented.ReporterFactories;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.ValueIndexEntryUpdate;
import org.neo4j.storageengine.api.schema.SimpleEntityValueClient;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueType;
import org.neo4j.values.storable.Values;

abstract class SimpleIndexAccessorCompatibility extends IndexAccessorCompatibility {
    SimpleIndexAccessorCompatibility(PropertyIndexProviderCompatibilityTestSuite testSuite, IndexPrototype prototype) {
        super(testSuite, prototype);
    }

    // This behaviour is shared by General and Unique indexes

    @Test
    void testIndexSeekByPrefix() throws Exception {
        updateAndCommit(asList(
                add(1L, descriptor, "a"),
                add(2L, descriptor, "A"),
                add(3L, descriptor, "apa"),
                add(4L, descriptor, "apA"),
                add(5L, descriptor, "b")));

        assertThat(query(PropertyIndexQuery.stringPrefix(1, stringValue("a")))).isEqualTo(asList(1L, 3L, 4L));
        assertThat(query(PropertyIndexQuery.stringPrefix(1, stringValue("A")))).isEqualTo(singletonList(2L));
        assertThat(query(PropertyIndexQuery.stringPrefix(1, stringValue("ba")))).isEqualTo(EMPTY_LIST);
        assertThat(query(PropertyIndexQuery.stringPrefix(1, stringValue("")))).isEqualTo(asList(1L, 2L, 3L, 4L, 5L));
    }

    @Test
    void testIndexSeekByPrefixOnNonStrings() throws Exception {
        updateAndCommit(asList(add(1L, descriptor, "2a"), add(2L, descriptor, 2L), add(2L, descriptor, 20L)));
        assertThat(query(PropertyIndexQuery.stringPrefix(1, stringValue("2")))).isEqualTo(singletonList(1L));
    }

    @Test
    void testIndexRangeSeekByDateTimeWithSneakyZones() throws Exception {
        DateTimeValue d1 = datetime(9999, 100, ZoneId.of("+18:00"));
        DateTimeValue d4 = datetime(10000, 100, ZoneId.of("UTC"));
        DateTimeValue d5 = datetime(10000, 100, ZoneId.of("+01:00"));
        DateTimeValue d6 = datetime(10000, 100, ZoneId.of("Europe/Stockholm"));
        DateTimeValue d7 = datetime(10000, 100, ZoneId.of("+03:00"));
        DateTimeValue d8 = datetime(10000, 101, ZoneId.of("UTC"));

        updateAndCommit(asList(
                add(1L, descriptor, d1),
                add(4L, descriptor, d4),
                add(5L, descriptor, d5),
                add(6L, descriptor, d6),
                add(7L, descriptor, d7),
                add(8L, descriptor, d8)));

        assertThat(query(range(1, d4, true, d7, true))).containsExactly(4L, 5L, 6L, 7L);
    }

    @Test
    void tracePageCacheAccessOnConsistencyCheck() {
        var pageCacheTracer = new DefaultPageCacheTracer();
        var contextFactory = new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER);
        accessor.consistencyCheck(
                ReporterFactories.noopReporterFactory(),
                contextFactory,
                Runtime.getRuntime().availableProcessors());

        assertThat(pageCacheTracer.pins()).isEqualTo(2);
        assertThat(pageCacheTracer.unpins()).isEqualTo(2);
        assertThat(pageCacheTracer.faults()).isEqualTo(2);
    }

    @Test
    void testIndexBoundingBoxSeek() throws Exception {
        assumeTrue(testSuite.supportsSpatial());
        assumeTrue(testSuite.supportsBoundingBoxQueries());

        PointValue p1 = Values.pointValue(CoordinateReferenceSystem.WGS_84, -180, -1);
        PointValue p2 = Values.pointValue(CoordinateReferenceSystem.WGS_84, -180, 1);
        PointValue p3 = Values.pointValue(CoordinateReferenceSystem.WGS_84, 0, 0);

        updateAndCommit(asList(add(1L, descriptor, p1), add(2L, descriptor, p2), add(3L, descriptor, p3)));

        assertThat(query(boundingBox(1, p1, p2))).containsExactly(1L, 2L);
    }

    @Test
    void shouldUpdateWithAllValues() throws Exception {
        // GIVEN
        List<ValueIndexEntryUpdate<?>> updates = updates(valueSet1);
        updateAndCommit(updates);

        // then
        int propertyKeyId = descriptor.schema().getPropertyId();
        for (NodeAndValue entry : valueSet1) {
            List<Long> result = query(PropertyIndexQuery.exact(propertyKeyId, entry.value));
            assertThat(result).isEqualTo(singletonList(entry.nodeId));
        }
    }

    @Test
    void shouldScanAllValues() throws Exception {
        // GIVEN
        List<ValueIndexEntryUpdate<?>> updates = updates(valueSet1);
        updateAndCommit(updates);
        Long[] allNodes = valueSet1.stream().map(x -> x.nodeId).toArray(Long[]::new);

        // THEN
        List<Long> result = query(PropertyIndexQuery.allEntries());
        assertThat(result).contains(allNodes);
    }

    @Test
    void shouldScanAllValuesThatExistWithPropKey() throws Exception {
        // GIVEN
        List<ValueIndexEntryUpdate<?>> updates = updates(valueSet1);
        updateAndCommit(updates);
        Long[] allNodes = valueSet1.stream().map(x -> x.nodeId).toArray(Long[]::new);

        // THEN
        int propertyKeyId = descriptor.schema().getPropertyId();
        List<Long> result = query(PropertyIndexQuery.exists(propertyKeyId));
        assertThat(result).contains(allNodes);
    }

    @Test
    void testIndexRangeSeekByNumber() throws Exception {
        testIndexRangeSeek(() -> random.randomValues().nextNumberValue());
    }

    @Test
    void testIndexRangeSeekByText() throws Exception {
        testIndexRangeSeek(() -> random.randomValues().nextTextValue());
    }

    @Test
    void testIndexRangeSeekByChar() throws Exception {
        testIndexRangeSeek(() -> random.randomValues().nextCharValue());
    }

    @Test
    void testIndexRangeSeekByDateTime() throws Exception {
        testIndexRangeSeek(() -> random.randomValues().nextDateTimeValue());
    }

    @Test
    void testIndexRangeSeekByLocalDateTime() throws Exception {
        testIndexRangeSeek(() -> random.randomValues().nextLocalDateTimeValue());
    }

    @Test
    void testIndexRangeSeekByDate() throws Exception {
        testIndexRangeSeek(() -> random.randomValues().nextDateValue());
    }

    @Test
    void testIndexRangeSeekByTime() throws Exception {
        testIndexRangeSeek(() -> random.randomValues().nextTimeValue());
    }

    @Test
    void testIndexRangeSeekByLocalTime() throws Exception {
        testIndexRangeSeek(() -> random.randomValues().nextLocalTimeValue());
    }

    // testIndexRangeSeekByDuration not present because duration is not orderable
    // testIndexRangeSeekByPeriod not present because period is not orderable
    // testIndexRangeSeekGeometry not present because geometry is not orderable
    // testIndexRangeSeekBoolean not present because test needs more than two possible values

    @Test
    void testIndexRangeSeekByZonedDateTimeArray() throws Exception {
        testIndexRangeSeekArray(() -> random.randomValues().nextDateTimeArray());
    }

    @Test
    void testIndexRangeSeekByLocalDateTimeArray() throws Exception {
        testIndexRangeSeekArray(() -> random.randomValues().nextLocalDateTimeArray());
    }

    @Test
    void testIndexRangeSeekByDateArray() throws Exception {
        testIndexRangeSeekArray(() -> random.randomValues().nextDateArray());
    }

    @Test
    void testIndexRangeSeekByZonedTimeArray() throws Exception {
        testIndexRangeSeekArray(() -> random.randomValues().nextTimeArray());
    }

    @Test
    void testIndexRangeSeekByLocalTimeArray() throws Exception {
        testIndexRangeSeekArray(() -> random.randomValues().nextLocalTimeArray());
    }

    @Test
    void testIndexRangeSeekByTextArray() throws Exception {
        testIndexRangeSeekArray(() -> random.randomValues().nextBasicMultilingualPlaneTextArray());
    }

    @Test
    void testIndexRangeSeekByCharArray() throws Exception {
        testIndexRangeSeekArray(() -> random.randomValues().nextCharArray());
    }

    @Test
    void testIndexRangeSeekByBooleanArray() throws Exception {
        testIndexRangeSeekArray(() -> random.randomValues().nextBooleanArray());
    }

    @Test
    void testIndexRangeSeekByByteArray() throws Exception {
        testIndexRangeSeekArray(() -> random.randomValues().nextByteArray());
    }

    @Test
    void testIndexRangeSeekByShortArray() throws Exception {
        testIndexRangeSeekArray(() -> random.randomValues().nextShortArray());
    }

    @Test
    void testIndexRangeSeekByIntArray() throws Exception {
        testIndexRangeSeekArray(() -> random.randomValues().nextIntArray());
    }

    @Test
    void testIndexRangeSeekByLongArray() throws Exception {
        testIndexRangeSeekArray(() -> random.randomValues().nextLongArray());
    }

    @Test
    void testIndexRangeSeekByFloatArray() throws Exception {
        testIndexRangeSeekArray(() -> random.randomValues().nextFloatArray());
    }

    @Test
    void testIndexRangeSeekByDoubleArray() throws Exception {
        testIndexRangeSeekArray(() -> random.randomValues().nextDoubleArray());
    }

    private void testIndexRangeSeekArray(Supplier<ArrayValue> generator) throws Exception {
        assumeTrue(testSuite.supportsGranularCompositeQueries());
        testIndexRangeSeek(generator);
    }

    private void testIndexRangeSeek(Supplier<? extends Value> generator) throws Exception {
        int count = random.nextInt(5, 10);
        List<Value> values = new ArrayList<>();
        List<ValueIndexEntryUpdate<?>> updates = new ArrayList<>();
        Set<Value> duplicateCheck = new HashSet<>();
        for (int i = 0; i < count; i++) {
            Value value;
            do {
                value = generator.get();
            } while (!duplicateCheck.add(value));
            values.add(value);
        }
        values.sort(Values.COMPARATOR);
        for (int i = 0; i < count; i++) {
            updates.add(add(i + 1, descriptor, values.get(i)));
        }
        Collections.shuffle(updates); // <- Don't rely on insert order

        updateAndCommit(updates);

        for (int f = 0; f < values.size(); f++) {
            for (int t = f; t < values.size(); t++) {
                Value from = values.get(f);
                Value to = values.get(t);
                for (boolean fromInclusive : new boolean[] {true, false}) {
                    for (boolean toInclusive : new boolean[] {true, false}) {
                        assertThat(query(range(1, from, fromInclusive, to, toInclusive)))
                                .isEqualTo(ids(f, fromInclusive, t, toInclusive));
                    }
                }
            }
        }
    }

    private static List<Long> ids(int fromIndex, boolean fromInclusive, int toIndex, boolean toInclusive) {
        List<Long> ids = new ArrayList<>();
        int from = fromInclusive ? fromIndex : fromIndex + 1;
        int to = toInclusive ? toIndex : toIndex - 1;
        for (int i = from; i <= to; i++) {
            ids.add((long) (i + 1));
        }
        return ids;
    }

    @Test
    void shouldRangeSeekInOrderAscendingNumber() throws Exception {
        Object o0 = 0;
        Object o1 = 1;
        Object o2 = 2;
        Object o3 = 3;
        Object o4 = 4;
        Object o5 = 5;
        shouldRangeSeekInOrder(IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderDescendingNumber() throws Exception {
        Object o0 = 0;
        Object o1 = 1;
        Object o2 = 2;
        Object o3 = 3;
        Object o4 = 4;
        Object o5 = 5;
        shouldRangeSeekInOrder(IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderAscendingString() throws Exception {
        Object o0 = "0";
        Object o1 = "1";
        Object o2 = "2";
        Object o3 = "3";
        Object o4 = "4";
        Object o5 = "5";
        shouldRangeSeekInOrder(IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderDescendingString() throws Exception {
        Object o0 = "0";
        Object o1 = "1";
        Object o2 = "2";
        Object o3 = "3";
        Object o4 = "4";
        Object o5 = "5";
        shouldRangeSeekInOrder(IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderAscendingDate() throws Exception {
        Object o0 = DateValue.epochDateRaw(0);
        Object o1 = DateValue.epochDateRaw(1);
        Object o2 = DateValue.epochDateRaw(2);
        Object o3 = DateValue.epochDateRaw(3);
        Object o4 = DateValue.epochDateRaw(4);
        Object o5 = DateValue.epochDateRaw(5);
        shouldRangeSeekInOrder(IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderDescendingDate() throws Exception {
        Object o0 = DateValue.epochDateRaw(0);
        Object o1 = DateValue.epochDateRaw(1);
        Object o2 = DateValue.epochDateRaw(2);
        Object o3 = DateValue.epochDateRaw(3);
        Object o4 = DateValue.epochDateRaw(4);
        Object o5 = DateValue.epochDateRaw(5);
        shouldRangeSeekInOrder(IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderAscendingLocalTime() throws Exception {
        Object o0 = LocalTimeValue.localTimeRaw(0);
        Object o1 = LocalTimeValue.localTimeRaw(1);
        Object o2 = LocalTimeValue.localTimeRaw(2);
        Object o3 = LocalTimeValue.localTimeRaw(3);
        Object o4 = LocalTimeValue.localTimeRaw(4);
        Object o5 = LocalTimeValue.localTimeRaw(5);
        shouldRangeSeekInOrder(IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderDescendingLocalTime() throws Exception {
        Object o0 = LocalTimeValue.localTimeRaw(0);
        Object o1 = LocalTimeValue.localTimeRaw(1);
        Object o2 = LocalTimeValue.localTimeRaw(2);
        Object o3 = LocalTimeValue.localTimeRaw(3);
        Object o4 = LocalTimeValue.localTimeRaw(4);
        Object o5 = LocalTimeValue.localTimeRaw(5);
        shouldRangeSeekInOrder(IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderAscendingTime() throws Exception {
        Object o0 = TimeValue.timeRaw(0, ZoneOffset.ofHours(0));
        Object o1 = TimeValue.timeRaw(1, ZoneOffset.ofHours(0));
        Object o2 = TimeValue.timeRaw(2, ZoneOffset.ofHours(0));
        Object o3 = TimeValue.timeRaw(3, ZoneOffset.ofHours(0));
        Object o4 = TimeValue.timeRaw(4, ZoneOffset.ofHours(0));
        Object o5 = TimeValue.timeRaw(5, ZoneOffset.ofHours(0));
        shouldRangeSeekInOrder(IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderDescendingTime() throws Exception {
        Object o0 = TimeValue.timeRaw(0, ZoneOffset.ofHours(0));
        Object o1 = TimeValue.timeRaw(1, ZoneOffset.ofHours(0));
        Object o2 = TimeValue.timeRaw(2, ZoneOffset.ofHours(0));
        Object o3 = TimeValue.timeRaw(3, ZoneOffset.ofHours(0));
        Object o4 = TimeValue.timeRaw(4, ZoneOffset.ofHours(0));
        Object o5 = TimeValue.timeRaw(5, ZoneOffset.ofHours(0));
        shouldRangeSeekInOrder(IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderAscendingLocalDateTime() throws Exception {
        Object o0 = LocalDateTimeValue.localDateTimeRaw(10, 0);
        Object o1 = LocalDateTimeValue.localDateTimeRaw(10, 1);
        Object o2 = LocalDateTimeValue.localDateTimeRaw(10, 2);
        Object o3 = LocalDateTimeValue.localDateTimeRaw(10, 3);
        Object o4 = LocalDateTimeValue.localDateTimeRaw(10, 4);
        Object o5 = LocalDateTimeValue.localDateTimeRaw(10, 5);
        shouldRangeSeekInOrder(IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderDescendingLocalDateTime() throws Exception {
        Object o0 = LocalDateTimeValue.localDateTimeRaw(10, 0);
        Object o1 = LocalDateTimeValue.localDateTimeRaw(10, 1);
        Object o2 = LocalDateTimeValue.localDateTimeRaw(10, 2);
        Object o3 = LocalDateTimeValue.localDateTimeRaw(10, 3);
        Object o4 = LocalDateTimeValue.localDateTimeRaw(10, 4);
        Object o5 = LocalDateTimeValue.localDateTimeRaw(10, 5);
        shouldRangeSeekInOrder(IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderAscendingDateTime() throws Exception {
        Object o0 = DateTimeValue.datetimeRaw(1, 0, ZoneId.of("UTC"));
        Object o1 = DateTimeValue.datetimeRaw(1, 1, ZoneId.of("UTC"));
        Object o2 = DateTimeValue.datetimeRaw(1, 2, ZoneId.of("UTC"));
        Object o3 = DateTimeValue.datetimeRaw(1, 3, ZoneId.of("UTC"));
        Object o4 = DateTimeValue.datetimeRaw(1, 4, ZoneId.of("UTC"));
        Object o5 = DateTimeValue.datetimeRaw(1, 5, ZoneId.of("UTC"));
        shouldRangeSeekInOrder(IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderDescendingDateTime() throws Exception {
        Object o0 = DateTimeValue.datetimeRaw(1, 0, ZoneId.of("UTC"));
        Object o1 = DateTimeValue.datetimeRaw(1, 1, ZoneId.of("UTC"));
        Object o2 = DateTimeValue.datetimeRaw(1, 2, ZoneId.of("UTC"));
        Object o3 = DateTimeValue.datetimeRaw(1, 3, ZoneId.of("UTC"));
        Object o4 = DateTimeValue.datetimeRaw(1, 4, ZoneId.of("UTC"));
        Object o5 = DateTimeValue.datetimeRaw(1, 5, ZoneId.of("UTC"));
        shouldRangeSeekInOrder(IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderAscendingNumberArray() throws Exception {
        Object o0 = new int[] {0};
        Object o1 = new int[] {1};
        Object o2 = new int[] {2};
        Object o3 = new int[] {3};
        Object o4 = new int[] {4};
        Object o5 = new int[] {5};
        shouldRangeSeekInOrder(IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderDescendingNumberArray() throws Exception {
        Object o0 = new int[] {0};
        Object o1 = new int[] {1};
        Object o2 = new int[] {2};
        Object o3 = new int[] {3};
        Object o4 = new int[] {4};
        Object o5 = new int[] {5};
        shouldRangeSeekInOrder(IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderAscendingStringArray() throws Exception {
        Object o0 = new String[] {"0"};
        Object o1 = new String[] {"1"};
        Object o2 = new String[] {"2"};
        Object o3 = new String[] {"3"};
        Object o4 = new String[] {"4"};
        Object o5 = new String[] {"5"};
        shouldRangeSeekInOrder(IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderDescendingStringArray() throws Exception {
        Object o0 = new String[] {"0"};
        Object o1 = new String[] {"1"};
        Object o2 = new String[] {"2"};
        Object o3 = new String[] {"3"};
        Object o4 = new String[] {"4"};
        Object o5 = new String[] {"5"};
        shouldRangeSeekInOrder(IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderAscendingBooleanArray() throws Exception {
        Object o0 = new boolean[] {false};
        Object o1 = new boolean[] {false, false};
        Object o2 = new boolean[] {false, true};
        Object o3 = new boolean[] {true};
        Object o4 = new boolean[] {true, false};
        Object o5 = new boolean[] {true, true};
        shouldRangeSeekInOrder(IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderDescendingBooleanArray() throws Exception {
        Object o0 = new boolean[] {false};
        Object o1 = new boolean[] {false, false};
        Object o2 = new boolean[] {false, true};
        Object o3 = new boolean[] {true};
        Object o4 = new boolean[] {true, false};
        Object o5 = new boolean[] {true, true};
        shouldRangeSeekInOrder(IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderAscendingDateTimeArray() throws Exception {
        Object o0 = new ZonedDateTime[] {ZonedDateTime.of(10, 10, 10, 10, 10, 10, 0, ZoneId.of("UTC"))};
        Object o1 = new ZonedDateTime[] {ZonedDateTime.of(10, 10, 10, 10, 10, 10, 1, ZoneId.of("UTC"))};
        Object o2 = new ZonedDateTime[] {ZonedDateTime.of(10, 10, 10, 10, 10, 10, 2, ZoneId.of("UTC"))};
        Object o3 = new ZonedDateTime[] {ZonedDateTime.of(10, 10, 10, 10, 10, 10, 3, ZoneId.of("UTC"))};
        Object o4 = new ZonedDateTime[] {ZonedDateTime.of(10, 10, 10, 10, 10, 10, 4, ZoneId.of("UTC"))};
        Object o5 = new ZonedDateTime[] {ZonedDateTime.of(10, 10, 10, 10, 10, 10, 5, ZoneId.of("UTC"))};
        shouldRangeSeekInOrder(IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderDescendingDateTimeArray() throws Exception {
        Object o0 = new ZonedDateTime[] {ZonedDateTime.of(10, 10, 10, 10, 10, 10, 0, ZoneId.of("UTC"))};
        Object o1 = new ZonedDateTime[] {ZonedDateTime.of(10, 10, 10, 10, 10, 10, 1, ZoneId.of("UTC"))};
        Object o2 = new ZonedDateTime[] {ZonedDateTime.of(10, 10, 10, 10, 10, 10, 2, ZoneId.of("UTC"))};
        Object o3 = new ZonedDateTime[] {ZonedDateTime.of(10, 10, 10, 10, 10, 10, 3, ZoneId.of("UTC"))};
        Object o4 = new ZonedDateTime[] {ZonedDateTime.of(10, 10, 10, 10, 10, 10, 4, ZoneId.of("UTC"))};
        Object o5 = new ZonedDateTime[] {ZonedDateTime.of(10, 10, 10, 10, 10, 10, 5, ZoneId.of("UTC"))};
        shouldRangeSeekInOrder(IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderAscendingLocalDateTimeArray() throws Exception {
        Object o0 = new LocalDateTime[] {LocalDateTime.of(10, 10, 10, 10, 10, 10, 0)};
        Object o1 = new LocalDateTime[] {LocalDateTime.of(10, 10, 10, 10, 10, 10, 1)};
        Object o2 = new LocalDateTime[] {LocalDateTime.of(10, 10, 10, 10, 10, 10, 2)};
        Object o3 = new LocalDateTime[] {LocalDateTime.of(10, 10, 10, 10, 10, 10, 3)};
        Object o4 = new LocalDateTime[] {LocalDateTime.of(10, 10, 10, 10, 10, 10, 4)};
        Object o5 = new LocalDateTime[] {LocalDateTime.of(10, 10, 10, 10, 10, 10, 5)};
        shouldRangeSeekInOrder(IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderDescendingLocalDateTimeArray() throws Exception {
        Object o0 = new LocalDateTime[] {LocalDateTime.of(10, 10, 10, 10, 10, 10, 0)};
        Object o1 = new LocalDateTime[] {LocalDateTime.of(10, 10, 10, 10, 10, 10, 1)};
        Object o2 = new LocalDateTime[] {LocalDateTime.of(10, 10, 10, 10, 10, 10, 2)};
        Object o3 = new LocalDateTime[] {LocalDateTime.of(10, 10, 10, 10, 10, 10, 3)};
        Object o4 = new LocalDateTime[] {LocalDateTime.of(10, 10, 10, 10, 10, 10, 4)};
        Object o5 = new LocalDateTime[] {LocalDateTime.of(10, 10, 10, 10, 10, 10, 5)};
        shouldRangeSeekInOrder(IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderAscendingTimeArray() throws Exception {
        Object o0 = new OffsetTime[] {OffsetTime.of(10, 10, 10, 0, ZoneOffset.ofHours(0))};
        Object o1 = new OffsetTime[] {OffsetTime.of(10, 10, 10, 1, ZoneOffset.ofHours(0))};
        Object o2 = new OffsetTime[] {OffsetTime.of(10, 10, 10, 2, ZoneOffset.ofHours(0))};
        Object o3 = new OffsetTime[] {OffsetTime.of(10, 10, 10, 3, ZoneOffset.ofHours(0))};
        Object o4 = new OffsetTime[] {OffsetTime.of(10, 10, 10, 4, ZoneOffset.ofHours(0))};
        Object o5 = new OffsetTime[] {OffsetTime.of(10, 10, 10, 5, ZoneOffset.ofHours(0))};
        shouldRangeSeekInOrder(IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderDescendingTimeArray() throws Exception {
        Object o0 = new OffsetTime[] {OffsetTime.of(10, 10, 10, 0, ZoneOffset.ofHours(0))};
        Object o1 = new OffsetTime[] {OffsetTime.of(10, 10, 10, 1, ZoneOffset.ofHours(0))};
        Object o2 = new OffsetTime[] {OffsetTime.of(10, 10, 10, 2, ZoneOffset.ofHours(0))};
        Object o3 = new OffsetTime[] {OffsetTime.of(10, 10, 10, 3, ZoneOffset.ofHours(0))};
        Object o4 = new OffsetTime[] {OffsetTime.of(10, 10, 10, 4, ZoneOffset.ofHours(0))};
        Object o5 = new OffsetTime[] {OffsetTime.of(10, 10, 10, 5, ZoneOffset.ofHours(0))};
        shouldRangeSeekInOrder(IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderAscendingDateArray() throws Exception {
        Object o0 = new LocalDate[] {LocalDate.of(10, 10, 1)};
        Object o1 = new LocalDate[] {LocalDate.of(10, 10, 2)};
        Object o2 = new LocalDate[] {LocalDate.of(10, 10, 3)};
        Object o3 = new LocalDate[] {LocalDate.of(10, 10, 4)};
        Object o4 = new LocalDate[] {LocalDate.of(10, 10, 5)};
        Object o5 = new LocalDate[] {LocalDate.of(10, 10, 6)};
        shouldRangeSeekInOrder(IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderDescendingDateArray() throws Exception {
        Object o0 = new LocalDate[] {LocalDate.of(10, 10, 1)};
        Object o1 = new LocalDate[] {LocalDate.of(10, 10, 2)};
        Object o2 = new LocalDate[] {LocalDate.of(10, 10, 3)};
        Object o3 = new LocalDate[] {LocalDate.of(10, 10, 4)};
        Object o4 = new LocalDate[] {LocalDate.of(10, 10, 5)};
        Object o5 = new LocalDate[] {LocalDate.of(10, 10, 6)};
        shouldRangeSeekInOrder(IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderAscendingLocalTimeArray() throws Exception {
        Object o0 = new LocalTime[] {LocalTime.of(10, 0)};
        Object o1 = new LocalTime[] {LocalTime.of(10, 1)};
        Object o2 = new LocalTime[] {LocalTime.of(10, 2)};
        Object o3 = new LocalTime[] {LocalTime.of(10, 3)};
        Object o4 = new LocalTime[] {LocalTime.of(10, 4)};
        Object o5 = new LocalTime[] {LocalTime.of(10, 5)};
        shouldRangeSeekInOrder(IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderDescendingLocalTimeArray() throws Exception {
        Object o0 = new LocalTime[] {LocalTime.of(10, 0)};
        Object o1 = new LocalTime[] {LocalTime.of(10, 1)};
        Object o2 = new LocalTime[] {LocalTime.of(10, 2)};
        Object o3 = new LocalTime[] {LocalTime.of(10, 3)};
        Object o4 = new LocalTime[] {LocalTime.of(10, 4)};
        Object o5 = new LocalTime[] {LocalTime.of(10, 5)};
        shouldRangeSeekInOrder(IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekAscendingWithoutFindingNanForOpenEnd() throws Exception {
        Object o0 = 0;
        Object o1 = 1.0;
        Object o2 = 2.5;
        Object o3 = 3;
        Object o4 = 4;
        Object o5 = 5;
        Object o6 = Double.POSITIVE_INFINITY;
        Object o7 = Double.NaN;

        shouldRangeSeekInOrderWithExpectedSize(
                IndexOrder.ASCENDING, RangeSeekMode.OPEN_END, 7, o0, o1, o2, o3, o4, o5, o6, o7);
    }

    @Test
    void shouldRangeSeekDescendingWithoutFindingNanForOpenEnd() throws Exception {
        Object o0 = 0;
        Object o1 = 1.0;
        Object o2 = 2.5;
        Object o3 = 3;
        Object o4 = 4;
        Object o5 = 5;
        Object o6 = Double.POSITIVE_INFINITY;
        Object o7 = Double.NaN;

        shouldRangeSeekInOrderWithExpectedSize(
                IndexOrder.DESCENDING, RangeSeekMode.OPEN_END, 7, o0, o1, o2, o3, o4, o5, o6, o7);
    }

    @Test
    void shouldRangeSeekAscendingWithoutFindingNanForOpenStart() throws Exception {
        Object o0 = Double.NaN;
        Object o1 = 1.0;
        Object o2 = 2.5;
        Object o3 = 3;
        Object o4 = 4;
        Object o5 = 5;
        Object o6 = Double.POSITIVE_INFINITY;

        shouldRangeSeekInOrderWithExpectedSize(
                IndexOrder.ASCENDING, RangeSeekMode.OPEN_START, 6, o0, o1, o2, o3, o4, o5, o6);
    }

    @Test
    void shouldRangeSeekDescendingWithoutFindingNanForOpenStart() throws Exception {
        Object o0 = Double.NaN;
        Object o1 = 1.0;
        Object o2 = 2.5;
        Object o3 = 3;
        Object o4 = 4;
        Object o5 = 5;
        Object o6 = Double.POSITIVE_INFINITY;

        shouldRangeSeekInOrderWithExpectedSize(
                IndexOrder.DESCENDING, RangeSeekMode.OPEN_START, 6, o0, o1, o2, o3, o4, o5, o6);
    }

    // Exact match with extreme values (NaN, +Inf, -Inf)
    @Test
    void shouldExactMatchPositiveInfinity() throws Exception {
        updateAndCommit(List.of(add(1L, descriptor, Double.POSITIVE_INFINITY)));
        assertThat(query(exact(1, Double.POSITIVE_INFINITY))).containsExactly(1L);
    }

    @Test
    void shouldExactMatchNegativeInfinity() throws Exception {
        updateAndCommit(List.of(add(1L, descriptor, Double.NEGATIVE_INFINITY)));
        assertThat(query(exact(1, Double.NEGATIVE_INFINITY))).containsExactly(1L);
    }

    @Test
    void shouldNotExactMatchNaN() throws Exception {
        updateAndCommit(List.of(add(1L, descriptor, Double.NaN)));
        assertThat(query(exact(1, Double.NaN))).isEmpty();
    }

    // Range with +Inf
    @Test
    void shouldFindPositiveInfinityInOpenEndRange() throws Exception {
        updateAndCommit(List.of(add(1L, descriptor, Double.POSITIVE_INFINITY)));
        assertThat(query(range(1, 0, true, null, false))).containsExactly(1L);
    }

    @Test
    void shouldFindPositiveInfinityInRangeToPositiveInfinityInclusive() throws Exception {
        updateAndCommit(List.of(add(1L, descriptor, Double.POSITIVE_INFINITY)));
        assertThat(query(range(1, 0, true, Double.POSITIVE_INFINITY, true))).containsExactly(1L);
    }

    @Test
    void shouldNotFindPositiveInfinityInRangeToPositiveInfinityExclusive() throws Exception {
        updateAndCommit(List.of(add(1L, descriptor, Double.POSITIVE_INFINITY)));
        assertThat(query(range(1, 0, true, Double.POSITIVE_INFINITY, false))).isEmpty();
    }

    // Range with -Inf
    @Test
    void shouldFindNegativeInfinityInOpenStartRange() throws Exception {
        updateAndCommit(List.of(add(1L, descriptor, Double.NEGATIVE_INFINITY)));
        assertThat(query(range(1, null, true, 0, false))).containsExactly(1L);
    }

    @Test
    void shouldFindNegativeInfinityInRangeFromNegativeInfinityInclusive() throws Exception {
        updateAndCommit(List.of(add(1L, descriptor, Double.NEGATIVE_INFINITY)));
        assertThat(query(range(1, Double.NEGATIVE_INFINITY, true, 0, false))).containsExactly(1L);
    }

    @Test
    void shouldNotFindNegativeInfinityInRangeFromNegativeInfinityExclusive() throws Exception {
        updateAndCommit(List.of(add(1L, descriptor, Double.NEGATIVE_INFINITY)));
        assertThat(query(range(1, Double.NEGATIVE_INFINITY, false, 0, false))).isEmpty();
    }

    // Range with NaN
    @Test
    void shouldNotFindNaNInAnyRange() throws Exception {
        updateAndCommit(List.of(add(1L, descriptor, Double.NaN)));
        assertThat(query(range(1, Double.NaN, true, null, true))).isEmpty();
        assertThat(query(range(1, Double.NaN, true, null, false))).isEmpty();
        assertThat(query(range(1, Double.NaN, false, null, true))).isEmpty();
        assertThat(query(range(1, Double.NaN, false, null, false))).isEmpty();
        assertThat(query(range(1, null, true, Double.NaN, true))).isEmpty();
        assertThat(query(range(1, null, true, Double.NaN, false))).isEmpty();
        assertThat(query(range(1, null, false, Double.NaN, true))).isEmpty();
        assertThat(query(range(1, null, false, Double.NaN, false))).isEmpty();
    }

    @Test
    void shouldNotFindAnythingInRangeWithToNaN() throws Exception {
        updateAndCommit(List.of(
                add(1L, descriptor, Double.NEGATIVE_INFINITY),
                add(2L, descriptor, Long.MIN_VALUE),
                add(3L, descriptor, 0),
                add(4L, descriptor, Long.MAX_VALUE),
                add(5L, descriptor, Double.POSITIVE_INFINITY),
                add(6L, descriptor, Double.NaN)));
        assertThat(query(range(1, null, true, Double.NaN, true))).isEmpty();
        assertThat(query(range(1, null, false, Double.NaN, false))).isEmpty();
        assertThat(query(range(1, Double.NaN, true, null, true))).isEmpty();
        assertThat(query(range(1, Double.NaN, false, null, false))).isEmpty();
    }

    // Exists with extreme values (NaN, +Inf, -Inf)
    @Test
    void shouldFindExtremeValueInExistsScan() throws Exception {
        updateAndCommit(List.of(
                add(3L, descriptor, Double.NEGATIVE_INFINITY),
                add(2L, descriptor, Double.POSITIVE_INFINITY),
                add(1L, descriptor, Double.NaN)));
        assertThat(queryNoSort(exists(1))).containsExactly(3L, 2L, 1L);
    }

    // Index scan (all entries) with extreme values (NaN, +Inf, -Inf)
    @Test
    void shouldFindExtremeValueInAllEntriesScan() throws Exception {
        Object o0 = Double.NaN;
        Object o1 = Double.POSITIVE_INFINITY;
        Object o2 = Double.NEGATIVE_INFINITY;

        updateAndCommit(List.of(add(1L, descriptor, o0), add(2L, descriptor, o1), add(3L, descriptor, o2)));

        assertThat(queryNoSort(allEntries())).containsExactly(3L, 2L, 1L);
    }

    private void shouldRangeSeekInOrder(IndexOrder order, Object... objects) throws Exception {
        shouldRangeSeekInOrderWithExpectedSize(order, RangeSeekMode.CLOSED, objects.length, objects);
    }

    private void shouldRangeSeekInOrderWithExpectedSize(
            IndexOrder order, RangeSeekMode rangeSeekMode, int expectedSize, Object... objects) throws Exception {
        PropertyIndexQuery range =
                switch (rangeSeekMode) {
                    case CLOSED -> range(
                            100, Values.of(objects[0]), true, Values.of(objects[objects.length - 1]), true);
                    case OPEN_END -> range(100, Values.of(objects[0]), true, null, false);
                    case OPEN_START -> range(100, null, false, Values.of(objects[objects.length - 1]), true);
                };

        if (order == IndexOrder.ASCENDING || order == IndexOrder.DESCENDING) {
            assumeTrue(descriptor.getCapability().supportsOrdering(), "Assume support for order " + order);
        }

        List<ValueIndexEntryUpdate<?>> additions =
                Arrays.stream(objects).map(o -> add(1, descriptor, o)).collect(Collectors.toList());
        Collections.shuffle(additions, random.random());
        updateAndCommit(additions);

        SimpleEntityValueClient client = new SimpleEntityValueClient();
        try (AutoCloseable ignored = query(client, order, range)) {
            List<Long> seenIds = assertClientReturnValuesInOrder(client, order);
            assertThat(seenIds.size()).isEqualTo(expectedSize);
        }
    }

    @Test
    void shouldUpdateEntries() throws Exception {
        ValueType[] valueTypes = testSuite.supportedValueTypes();
        long entityId = random.nextLong(1_000_000_000);
        for (ValueType valueType : valueTypes) {
            // given
            Value value = random.nextValue(valueType);
            updateAndCommit(singletonList(IndexEntryUpdate.add(entityId, descriptor, value)));
            assertEquals(singletonList(entityId), query(PropertyIndexQuery.exact(0, value)));

            // when
            Value newValue;
            do {
                newValue = random.nextValue(valueType);
            } while (value.equals(newValue));
            updateAndCommit(singletonList(IndexEntryUpdate.change(entityId, descriptor, value, newValue)));

            // then
            assertEquals(emptyList(), query(PropertyIndexQuery.exact(0, value)));
            assertEquals(singletonList(entityId), query(PropertyIndexQuery.exact(0, newValue)));
        }
    }

    @Test
    void shouldRemoveEntries() throws Exception {
        ValueType[] valueTypes = testSuite.supportedValueTypes();
        long entityId = random.nextLong(1_000_000_000);
        for (ValueType valueType : valueTypes) {
            // given
            Value value = random.nextValue(valueType);
            updateAndCommit(singletonList(IndexEntryUpdate.add(entityId, descriptor, value)));
            assertEquals(singletonList(entityId), query(PropertyIndexQuery.exact(0, value)));

            // when
            updateAndCommit(singletonList(IndexEntryUpdate.remove(entityId, descriptor, value)));

            // then
            assertTrue(query(PropertyIndexQuery.exact(0, value)).isEmpty());
        }
    }

    // This behaviour is expected by General indexes
    abstract static class General extends SimpleIndexAccessorCompatibility {
        General(PropertyIndexProviderCompatibilityTestSuite testSuite) {
            super(testSuite, testSuite.indexPrototype());
        }

        @Test
        void closingAnOnlineIndexUpdaterMustNotThrowEvenIfItHasBeenFedConflictingData() throws Exception {
            // The reason is that we use and close IndexUpdaters in commit - not in prepare - and therefor
            // we cannot have them go around and throw exceptions, because that could potentially break
            // recovery.
            // Conflicting data can happen because of faulty data coercion. These faults are resolved by
            // the exact-match filtering we do on index seeks.

            updateAndCommit(asList(add(1L, descriptor, "a"), add(2L, descriptor, "a")));

            assertThat(query(exact(1, "a"))).containsExactly(1L, 2L);
        }

        @Test
        void testIndexSeekAndScan() throws Exception {
            updateAndCommit(asList(add(1L, descriptor, "a"), add(2L, descriptor, "a"), add(3L, descriptor, "b")));

            assertThat(query(exact(1, "a"))).containsExactly(1L, 2L);
            assertThat(query(exists(1))).containsExactly(1L, 2L, 3L);
        }

        @Test
        void testIndexRangeSeekByNumberWithDuplicates() throws Exception {
            updateAndCommit(asList(
                    add(1L, descriptor, -5),
                    add(2L, descriptor, -5),
                    add(3L, descriptor, 0),
                    add(4L, descriptor, 5),
                    add(5L, descriptor, 5)));

            assertThat(query(range(1, -5, true, 5, true))).containsExactly(1L, 2L, 3L, 4L, 5L);
            assertThat(query(range(1, -3, true, -1, true))).isEmpty();
            assertThat(query(range(1, -5, true, 4, true))).containsExactly(1L, 2L, 3L);
            assertThat(query(range(1, -4, true, 5, true))).containsExactly(3L, 4L, 5L);
            assertThat(query(range(1, -5, true, 5, true))).containsExactly(1L, 2L, 3L, 4L, 5L);
        }

        @Test
        void testIndexRangeSeekByStringWithDuplicates() throws Exception {
            updateAndCommit(asList(
                    add(1L, descriptor, "Anna"),
                    add(2L, descriptor, "Anna"),
                    add(3L, descriptor, "Bob"),
                    add(4L, descriptor, "William"),
                    add(5L, descriptor, "William")));

            assertThat(query(range(1, "Anna", false, "William", false))).containsExactly(3L);
            assertThat(query(range(1, "Arabella", false, "Bob", false))).isEmpty();
            assertThat(query(range(1, "Anna", true, "William", false))).containsExactly(1L, 2L, 3L);
            assertThat(query(range(1, "Anna", false, "William", true))).containsExactly(3L, 4L, 5L);
            assertThat(query(range(1, "Anna", true, "William", true))).containsExactly(1L, 2L, 3L, 4L, 5L);
        }

        @Test
        void testIndexRangeSeekByDateWithDuplicates() throws Exception {
            testIndexRangeSeekWithDuplicates(epochDate(100), epochDate(101), epochDate(200), epochDate(300));
        }

        @Test
        void testIndexRangeSeekByLocalDateTimeWithDuplicates() throws Exception {
            testIndexRangeSeekWithDuplicates(
                    localDateTime(1000, 10), localDateTime(1000, 11), localDateTime(2000, 10), localDateTime(3000, 10));
        }

        @Test
        void testIndexRangeSeekByDateTimeWithDuplicates() throws Exception {
            testIndexRangeSeekWithDuplicates(
                    datetime(1000, 10, UTC), datetime(1000, 11, UTC), datetime(2000, 10, UTC), datetime(3000, 10, UTC));
        }

        @Test
        void testIndexRangeSeekByLocalTimeWithDuplicates() throws Exception {
            testIndexRangeSeekWithDuplicates(localTime(1000), localTime(1001), localTime(2000), localTime(3000));
        }

        @Test
        void testIndexRangeSeekByTimeWithDuplicates() throws Exception {
            testIndexRangeSeekWithDuplicates(time(1000, UTC), time(1001, UTC), time(2000, UTC), time(3000, UTC));
        }

        @Test
        void testIndexRangeSeekByTimeWithZonesAndDuplicates() throws Exception {
            testIndexRangeSeekWithDuplicates(
                    time(20, 31, 53, 4, ZoneOffset.of("+17:02")),
                    time(20, 31, 54, 3, ZoneOffset.of("+17:02")),
                    time(19, 31, 54, 2, UTC),
                    time(18, 23, 27, 1, ZoneOffset.of("-18:00")));
        }

        /**
         * Helper for testing range seeks. Takes 4 ordered sample values.
         */
        private <VALUE extends Value> void testIndexRangeSeekWithDuplicates(VALUE v1, VALUE v2, VALUE v3, VALUE v4)
                throws Exception {
            updateAndCommit(asList(
                    add(1L, descriptor, v1),
                    add(2L, descriptor, v1),
                    add(3L, descriptor, v3),
                    add(4L, descriptor, v4),
                    add(5L, descriptor, v4)));

            assertThat(query(range(1, v1, false, v4, false))).containsExactly(3L);
            assertThat(query(range(1, v2, false, v3, false))).isEmpty();
            assertThat(query(range(1, v1, true, v4, false))).containsExactly(1L, 2L, 3L);
            assertThat(query(range(1, v1, false, v4, true))).containsExactly(3L, 4L, 5L);
            assertThat(query(range(1, v1, true, v4, true))).containsExactly(1L, 2L, 3L, 4L, 5L);
        }

        @Test
        void testIndexRangeSeekByPrefixWithDuplicates() throws Exception {
            updateAndCommit(asList(
                    add(1L, descriptor, "a"),
                    add(2L, descriptor, "A"),
                    add(3L, descriptor, "apa"),
                    add(4L, descriptor, "apa"),
                    add(5L, descriptor, "apa")));

            assertThat(query(stringPrefix(1, stringValue("a")))).containsExactly(1L, 3L, 4L, 5L);
            assertThat(query(stringPrefix(1, stringValue("apa")))).containsExactly(3L, 4L, 5L);
        }

        @Test
        void testIndexFullSearchWithDuplicates() throws Exception {
            assumeTrue(testSuite.supportsContainsAndEndsWithQueries());

            updateAndCommit(asList(
                    add(1L, descriptor, "a"),
                    add(2L, descriptor, "A"),
                    add(3L, descriptor, "apa"),
                    add(4L, descriptor, "apa"),
                    add(5L, descriptor, "apalong"),
                    add(6L, descriptor, "apa apa")));

            assertThat(query(stringContains(1, stringValue("a")))).containsExactly(1L, 3L, 4L, 5L, 6L);
            assertThat(query(stringContains(1, stringValue("apa")))).containsExactly(3L, 4L, 5L, 6L);
            assertThat(query(stringContains(1, stringValue("apa*")))).isEmpty();
            assertThat(query(stringContains(1, stringValue("pa ap")))).containsExactly(6L);
        }

        @Test
        void testIndexEndsWithWithDuplicated() throws Exception {
            assumeTrue(testSuite.supportsContainsAndEndsWithQueries());

            updateAndCommit(asList(
                    add(1L, descriptor, "a"),
                    add(2L, descriptor, "A"),
                    add(3L, descriptor, "apa"),
                    add(4L, descriptor, "apa"),
                    add(5L, descriptor, "longapa"),
                    add(6L, descriptor, "apalong"),
                    add(7L, descriptor, "apa apa")));

            assertThat(query(stringSuffix(1, stringValue("a")))).containsExactly(1L, 3L, 4L, 5L, 7L);
            assertThat(query(stringSuffix(1, stringValue("apa")))).containsExactly(3L, 4L, 5L, 7L);
            assertThat(query(stringSuffix(1, stringValue("apa*")))).isEmpty();
            assertThat(query(stringSuffix(1, stringValue("a apa")))).containsExactly(7L);
        }

        @Test
        void testIndexShouldHandleLargeAmountOfDuplicatesStringArray() throws Exception {
            Value arrayValue = nextRandomValidArrayValue();
            doTestShouldHandleLargeAmountOfDuplicates(arrayValue);
        }

        private void doTestShouldHandleLargeAmountOfDuplicates(Object value) throws Exception {
            List<ValueIndexEntryUpdate<?>> updates = new ArrayList<>();
            List<Long> nodeIds = new ArrayList<>();
            for (long i = 0; i < 1000; i++) {
                nodeIds.add(i);
                updates.add(add(i, descriptor, value));
            }
            updateAndCommit(updates);

            assertThat(query(exists(1))).containsAll(nodeIds);
        }

        private Value nextRandomValidArrayValue() {
            Value value;
            do {
                value = random.randomValues().nextArray();
                // todo remove when spatial is supported by all
            } while (!testSuite.supportsSpatial() && Values.isGeometryArray(value));
            return value;
        }
    }

    // This behaviour is expected by Unique indexes
    abstract static class Unique extends SimpleIndexAccessorCompatibility {
        Unique(PropertyIndexProviderCompatibilityTestSuite testSuite) {
            super(testSuite, testSuite.uniqueIndexPrototype());
        }

        @Test
        void closingAnOnlineIndexUpdaterMustNotThrowEvenIfItHasBeenFedConflictingData() throws Exception {
            // The reason is that we use and close IndexUpdaters in commit - not in prepare - and therefor
            // we cannot have them go around and throw exceptions, because that could potentially break
            // recovery.
            // Conflicting data can happen because of faulty data coercion. These faults are resolved by
            // the exact-match filtering we do on index seeks.

            updateAndCommit(asList(add(1L, descriptor, "a"), add(2L, descriptor, "a")));

            assertThat(query(exact(1, "a"))).containsExactly(1L, 2L);
        }

        @Test
        void testIndexSeekAndScan() throws Exception {
            updateAndCommit(asList(add(1L, descriptor, "a"), add(2L, descriptor, "b"), add(3L, descriptor, "c")));

            assertThat(query(exact(1, "a"))).containsExactly(1L);
            assertThat(query(PropertyIndexQuery.exists(1))).containsExactly(1L, 2L, 3L);
        }
    }

    private enum RangeSeekMode {
        CLOSED,
        OPEN_END,
        OPEN_START
    }
}
