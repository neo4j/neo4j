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

import static java.time.LocalDate.ofEpochDay;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.neo4j.internal.helpers.collection.Iterables.single;
import static org.neo4j.internal.helpers.collection.Pair.of;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.allEntries;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.boundingBox;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.exact;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.exists;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.range;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.stringContains;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.stringPrefix;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.stringSuffix;
import static org.neo4j.internal.kernel.api.QueryContext.NULL_CONTEXT;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;
import static org.neo4j.kernel.impl.index.schema.IndexUsageTracking.NO_USAGE_TRACKING;
import static org.neo4j.values.storable.CoordinateReferenceSystem.CARTESIAN;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS_84;
import static org.neo4j.values.storable.DateTimeValue.datetime;
import static org.neo4j.values.storable.DateValue.epochDate;
import static org.neo4j.values.storable.Values.booleanArray;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.longArray;
import static org.neo4j.values.storable.Values.pointArray;
import static org.neo4j.values.storable.Values.pointValue;
import static org.neo4j.values.storable.Values.stringArray;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptorSupplier;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.ValueIndexEntryUpdate;
import org.neo4j.storageengine.api.schema.SimpleEntityValueClient;
import org.neo4j.test.InMemoryTokens;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueTuple;
import org.neo4j.values.storable.ValueType;
import org.neo4j.values.storable.Values;

abstract class CompositeIndexAccessorCompatibility extends IndexAccessorCompatibility {
    CompositeIndexAccessorCompatibility(
            PropertyIndexProviderCompatibilityTestSuite testSuite, IndexPrototype prototype) {
        super(testSuite, prototype);
    }

    @Test
    void testIndexScan() throws Exception {
        List<Long> ids = LongStream.rangeClosed(1L, 10L).boxed().toList();
        Supplier<Value> randomValue = () -> random.randomValues().nextValueOfTypes(testSuite.supportedValueTypes());

        updateAndCommit(ids.stream()
                .map(id -> add(id, descriptor.schema(), randomValue.get(), randomValue.get()))
                .collect(Collectors.toUnmodifiableList()));

        assertThat(query(allEntries())).isEqualTo(ids);
    }

    /* testIndexSeekAndScan */

    @Test
    void testIndexScanAndSeekExactWithExactByString() throws Exception {
        testIndexScanAndSeekExactWithExact("a", "b");
    }

    @Test
    void testIndexScanAndSeekExactWithExactByNumber() throws Exception {
        testIndexScanAndSeekExactWithExact(333, 101);
    }

    @Test
    void testIndexScanAndSeekExactWithExactByBoolean() throws Exception {
        testIndexScanAndSeekExactWithExact(true, false);
    }

    @Test
    void testIndexScanAndSeekExactWithExactByTemporal() throws Exception {
        testIndexScanAndSeekExactWithExact(epochDate(303), epochDate(101));
    }

    @Test
    void testIndexScanAndSeekExactWithExactByStringArray() throws Exception {
        testIndexScanAndSeekExactWithExact(new String[] {"a", "c"}, new String[] {"b", "c"});
    }

    @Test
    void testIndexScanAndSeekExactWithExactByNumberArray() throws Exception {
        testIndexScanAndSeekExactWithExact(new int[] {333, 900}, new int[] {101, 900});
    }

    @Test
    void testIndexScanAndSeekExactWithExactByBooleanArray() throws Exception {
        testIndexScanAndSeekExactWithExact(new boolean[] {true, true}, new boolean[] {false, true});
    }

    @Test
    void testIndexScanAndSeekExactWithExactByTemporalArray() throws Exception {
        testIndexScanAndSeekExactWithExact(dateArray(333, 900), dateArray(101, 900));
    }

    private void testIndexScanAndSeekExactWithExact(Object a, Object b) throws Exception {
        testIndexScanAndSeekExactWithExact(Values.of(a), Values.of(b));
    }

    private void testIndexScanAndSeekExactWithExact(Value a, Value b) throws Exception {
        updateAndCommit(asList(
                add(1L, descriptor.schema(), a, a),
                add(2L, descriptor.schema(), b, b),
                add(3L, descriptor.schema(), a, b)));

        assertThat(query(exact(0, a), exact(1, a))).isEqualTo(singletonList(1L));
        assertThat(query(exact(0, b), exact(1, b))).isEqualTo(singletonList(2L));
        assertThat(query(exact(0, a), exact(1, b))).isEqualTo(singletonList(3L));
        assertThat(query(exists(1))).isEqualTo(asList(1L, 2L, 3L));
    }

    @Test
    void testIndexScanAndSeekExactWithExactByPoint() throws Exception {
        assumeTrue(testSuite.supportsSpatial(), "Assume support for spatial");

        PointValue gps = pointValue(WGS_84, 12.6, 56.7);
        PointValue car = pointValue(CARTESIAN, 12.6, 56.7);
        PointValue gps3d = pointValue(CoordinateReferenceSystem.WGS_84_3D, 12.6, 56.7, 100.0);
        PointValue car3d = pointValue(CoordinateReferenceSystem.CARTESIAN_3D, 12.6, 56.7, 100.0);

        updateAndCommit(asList(
                add(1L, descriptor.schema(), gps, gps),
                add(2L, descriptor.schema(), car, car),
                add(3L, descriptor.schema(), gps, car),
                add(4L, descriptor.schema(), gps3d, gps3d),
                add(5L, descriptor.schema(), car3d, car3d),
                add(6L, descriptor.schema(), gps, car3d)));

        assertThat(query(exact(0, gps), exact(1, gps))).isEqualTo(singletonList(1L));
        assertThat(query(exact(0, car), exact(1, car))).isEqualTo(singletonList(2L));
        assertThat(query(exact(0, gps), exact(1, car))).isEqualTo(singletonList(3L));
        assertThat(query(exact(0, gps3d), exact(1, gps3d))).isEqualTo(singletonList(4L));
        assertThat(query(exact(0, car3d), exact(1, car3d))).isEqualTo(singletonList(5L));
        assertThat(query(exact(0, gps), exact(1, car3d))).isEqualTo(singletonList(6L));
        assertThat(query(exists(1))).isEqualTo(asList(1L, 2L, 3L, 4L, 5L, 6L));
    }

    /* testIndexExactAndRangeExact_Range */

    @Test
    void testIndexSeekExactWithRangeByString() throws Exception {
        testIndexSeekExactWithRange(
                Values.of("a"),
                Values.of("b"),
                Values.of("Anabelle"),
                Values.of("Anna"),
                Values.of("Bob"),
                Values.of("Harriet"),
                Values.of("William"));
    }

    @Test
    void testIndexSeekExactWithRangeByNumber() throws Exception {
        testIndexSeekExactWithRange(
                Values.of(303),
                Values.of(101),
                Values.of(111),
                Values.of(222),
                Values.of(333),
                Values.of(444),
                Values.of(555));
    }

    @Test
    void testIndexSeekExactWithRangeByTemporal() throws Exception {
        testIndexSeekExactWithRange(
                epochDate(303),
                epochDate(101),
                epochDate(111),
                epochDate(222),
                epochDate(333),
                epochDate(444),
                epochDate(555));
    }

    @Test
    void testIndexSeekExactWithRangeByBoolean() throws Exception {
        testIndexSeekExactWithRangeByBooleanType(
                BooleanValue.TRUE, BooleanValue.FALSE, BooleanValue.FALSE, BooleanValue.TRUE);
    }

    @Test
    void testIndexSeekExactWithRangeByStringArray() throws Exception {
        testIndexSeekExactWithRange(
                stringArray("a", "c"),
                stringArray("b", "c"),
                stringArray("Anabelle", "c"),
                stringArray("Anna", "c"),
                stringArray("Bob", "c"),
                stringArray("Harriet", "c"),
                stringArray("William", "c"));
    }

    @Test
    void testIndexSeekExactWithRangeByNumberArray() throws Exception {
        testIndexSeekExactWithRange(
                longArray(new long[] {333, 9000}),
                longArray(new long[] {101, 900}),
                longArray(new long[] {111, 900}),
                longArray(new long[] {222, 900}),
                longArray(new long[] {333, 900}),
                longArray(new long[] {444, 900}),
                longArray(new long[] {555, 900}));
    }

    @Test
    void testIndexSeekExactWithRangeByBooleanArray() throws Exception {
        testIndexSeekExactWithRange(
                booleanArray(new boolean[] {true, true}),
                booleanArray(new boolean[] {false, false}),
                booleanArray(new boolean[] {false, false}),
                booleanArray(new boolean[] {false, true}),
                booleanArray(new boolean[] {true, false}),
                booleanArray(new boolean[] {true, true}),
                booleanArray(new boolean[] {true, true, true}));
    }

    @Test
    void testIndexSeekExactWithRangeByTemporalArray() throws Exception {
        testIndexSeekExactWithRange(
                dateArray(303, 900),
                dateArray(101, 900),
                dateArray(111, 900),
                dateArray(222, 900),
                dateArray(333, 900),
                dateArray(444, 900),
                dateArray(555, 900));
    }

    @Test
    void testIndexSeekExactWithRangeBySpatial() throws Exception {
        testIndexSeekExactWithBoundingBox(
                intValue(100),
                intValue(10),
                pointValue(WGS_84, -10D, -10D),
                pointValue(WGS_84, -1D, -1D),
                pointValue(WGS_84, 0D, 0D),
                pointValue(WGS_84, 1D, 1D),
                pointValue(WGS_84, 10D, 10D));
    }

    private void testIndexSeekExactWithRange(
            Value base1, Value base2, Value obj1, Value obj2, Value obj3, Value obj4, Value obj5) throws Exception {
        assumeTrue(testSuite.supportsGranularCompositeQueries(), "Assume support for granular composite queries");

        updateAndCommit(asList(
                add(1L, descriptor.schema(), base1, obj1),
                add(2L, descriptor.schema(), base1, obj2),
                add(3L, descriptor.schema(), base1, obj3),
                add(4L, descriptor.schema(), base1, obj4),
                add(5L, descriptor.schema(), base1, obj5),
                add(6L, descriptor.schema(), base2, obj1),
                add(7L, descriptor.schema(), base2, obj2),
                add(8L, descriptor.schema(), base2, obj3),
                add(9L, descriptor.schema(), base2, obj4),
                add(10L, descriptor.schema(), base2, obj5)));

        assertThat(query(exact(0, base1), range(1, obj2, true, obj4, false))).containsExactly(2L, 3L);
        assertThat(query(exact(0, base1), range(1, obj4, true, null, false))).containsExactly(4L, 5L);
        assertThat(query(exact(0, base1), range(1, obj4, false, null, true))).containsExactly(5L);
        assertThat(query(exact(0, base1), range(1, obj5, false, obj2, true))).isEmpty();
        assertThat(query(exact(0, base1), range(1, null, false, obj3, false))).containsExactly(1L, 2L);
        assertThat(query(exact(0, base1), range(1, null, true, obj3, true))).containsExactly(1L, 2L, 3L);
        assertThat(query(exact(0, base1), range(1, obj1, false, obj2, true))).containsExactly(2L);
        assertThat(query(exact(0, base1), range(1, obj1, false, obj3, false))).containsExactly(2L);
        assertThat(query(exact(0, base2), range(1, obj2, true, obj4, false))).containsExactly(7L, 8L);
        assertThat(query(exact(0, base2), range(1, obj4, true, null, false))).containsExactly(9L, 10L);
        assertThat(query(exact(0, base2), range(1, obj4, false, null, true))).containsExactly(10L);
        assertThat(query(exact(0, base2), range(1, obj5, false, obj2, true))).isEmpty();
        assertThat(query(exact(0, base2), range(1, null, false, obj3, false))).containsExactly(6L, 7L);
        assertThat(query(exact(0, base2), range(1, null, true, obj3, true))).containsExactly(6L, 7L, 8L);
        assertThat(query(exact(0, base2), range(1, obj1, false, obj2, true))).containsExactly(7L);
        assertThat(query(exact(0, base2), range(1, obj1, false, obj3, false))).containsExactly(7L);
    }

    private void testIndexSeekExactWithBoundingBox(
            Value base1,
            Value base2,
            PointValue obj1,
            PointValue obj2,
            PointValue obj3,
            PointValue obj4,
            PointValue obj5)
            throws Exception {
        assumeTrue(testSuite.supportsGranularCompositeQueries(), "Assume support for granular composite queries");
        assumeTrue(testSuite.supportsSpatial(), "Assume support for spacial value types");
        assumeTrue(testSuite.supportsBoundingBoxQueries(), "Assume support for bounding box queries");

        updateAndCommit(asList(
                add(1L, descriptor.schema(), base1, obj1),
                add(2L, descriptor.schema(), base1, obj2),
                add(3L, descriptor.schema(), base1, obj3),
                add(4L, descriptor.schema(), base1, obj4),
                add(5L, descriptor.schema(), base1, obj5),
                add(6L, descriptor.schema(), base2, obj1),
                add(7L, descriptor.schema(), base2, obj2),
                add(8L, descriptor.schema(), base2, obj3),
                add(9L, descriptor.schema(), base2, obj4),
                add(10L, descriptor.schema(), base2, obj5)));

        assertThat(query(exact(0, base1), boundingBox(1, obj2, obj4))).containsExactly(2L, 3L, 4L);
        assertThat(query(exact(0, base1), boundingBox(1, obj5, obj2))).isEmpty();
        assertThat(query(exact(0, base1), boundingBox(1, obj1, obj2))).containsExactly(1L, 2L);
        assertThat(query(exact(0, base1), boundingBox(1, obj1, obj3))).containsExactly(1L, 2L, 3L);
        assertThat(query(exact(0, base2), boundingBox(1, obj2, obj4))).containsExactly(7L, 8L, 9L);
        assertThat(query(exact(0, base2), boundingBox(1, obj5, obj2))).isEmpty();
        assertThat(query(exact(0, base2), boundingBox(1, obj1, obj2))).containsExactly(6L, 7L);
        assertThat(query(exact(0, base2), boundingBox(1, obj1, obj3))).containsExactly(6L, 7L, 8L);
    }

    private void testIndexSeekExactWithRangeByBooleanType(
            BooleanValue base1, BooleanValue base2, BooleanValue obj1, BooleanValue obj2) throws Exception {
        assumeTrue(testSuite.supportsBooleanRangeQueries(), "Assume support for boolean range queries");

        updateAndCommit(asList(
                add(1L, descriptor.schema(), base1, obj1),
                add(2L, descriptor.schema(), base1, obj2),
                add(3L, descriptor.schema(), base2, obj1),
                add(4L, descriptor.schema(), base2, obj2)));

        assertThat(query(exact(0, base1), range(1, obj1, true, obj2, true))).containsExactly(1L, 2L);
        assertThat(query(exact(0, base1), range(1, obj1, false, obj2, true))).containsExactly(2L);
        assertThat(query(exact(0, base1), range(1, obj1, true, obj2, false))).containsExactly(1L);
        assertThat(query(exact(0, base1), range(1, obj1, false, obj2, false))).isEmpty();
        assertThat(query(exact(0, base1), range(1, null, true, obj2, true))).containsExactly(1L, 2L);
        assertThat(query(exact(0, base1), range(1, obj1, true, null, true))).containsExactly(1L, 2L);
        assertThat(query(exact(0, base1), range(1, obj2, true, obj1, true))).isEmpty();
        assertThat(query(exact(0, base2), range(1, obj1, true, obj2, true))).containsExactly(3L, 4L);
        assertThat(query(exact(0, base2), range(1, obj1, false, obj2, true))).containsExactly(4L);
        assertThat(query(exact(0, base2), range(1, obj1, true, obj2, false))).containsExactly(3L);
        assertThat(query(exact(0, base2), range(1, obj1, false, obj2, false))).isEmpty();
        assertThat(query(exact(0, base2), range(1, null, true, obj2, true))).containsExactly(3L, 4L);
        assertThat(query(exact(0, base2), range(1, obj1, true, null, true))).containsExactly(3L, 4L);
        assertThat(query(exact(0, base2), range(1, obj2, true, obj1, true))).isEmpty();
    }

    /* stringPrefix */

    @Test
    void testIndexSeekExactWithPrefixRangeByString() throws Exception {
        assumeTrue(testSuite.supportsGranularCompositeQueries(), "Assume support for granular composite queries");

        updateAndCommit(asList(
                add(1L, descriptor.schema(), "a", "a"),
                add(2L, descriptor.schema(), "a", "A"),
                add(3L, descriptor.schema(), "a", "apa"),
                add(4L, descriptor.schema(), "a", "apA"),
                add(5L, descriptor.schema(), "a", "b"),
                add(6L, descriptor.schema(), "b", "a"),
                add(7L, descriptor.schema(), "b", "A"),
                add(8L, descriptor.schema(), "b", "apa"),
                add(9L, descriptor.schema(), "b", "apA"),
                add(10L, descriptor.schema(), "b", "b")));

        assertThat(query(exact(0, "a"), stringPrefix(1, stringValue("a")))).containsExactly(1L, 3L, 4L);
        assertThat(query(exact(0, "a"), stringPrefix(1, stringValue("A")))).containsExactly(2L);
        assertThat(query(exact(0, "a"), stringPrefix(1, stringValue("ba")))).isEmpty();
        assertThat(query(exact(0, "a"), stringPrefix(1, stringValue("")))).containsExactly(1L, 2L, 3L, 4L, 5L);
        assertThat(query(exact(0, "b"), stringPrefix(1, stringValue("a")))).containsExactly(6L, 8L, 9L);
        assertThat(query(exact(0, "b"), stringPrefix(1, stringValue("A")))).containsExactly(7L);
        assertThat(query(exact(0, "b"), stringPrefix(1, stringValue("ba")))).isEmpty();
        assertThat(query(exact(0, "b"), stringPrefix(1, stringValue("")))).containsExactly(6L, 7L, 8L, 9L, 10L);
    }

    @Test
    void testIndexSeekPrefixRangeWithExistsByString() throws Exception {
        assumeTrue(testSuite.supportsGranularCompositeQueries(), "Assume support for granular composite queries");

        updateAndCommit(asList(
                add(1L, descriptor.schema(), "a", 1),
                add(2L, descriptor.schema(), "A", epochDate(2)),
                add(3L, descriptor.schema(), "apa", "..."),
                add(4L, descriptor.schema(), "apA", "someString"),
                add(5L, descriptor.schema(), "b", true),
                add(6L, descriptor.schema(), "a", 100),
                add(7L, descriptor.schema(), "A", epochDate(200)),
                add(8L, descriptor.schema(), "apa", "!!!"),
                add(9L, descriptor.schema(), "apA", "someOtherString"),
                add(10L, descriptor.schema(), "b", false)));

        assertThat(query(stringPrefix(0, stringValue("a")), exists(1))).containsExactly(1L, 3L, 4L, 6L, 8L, 9L);
        assertThat(query(stringPrefix(0, stringValue("A")), exists(1))).containsExactly(2L, 7L);
        assertThat(query(stringPrefix(0, stringValue("ba")), exists(1))).isEmpty();
        assertThat(query(stringPrefix(0, stringValue("")), exists(1)))
                .containsExactly(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L);
    }

    /* testIndexSeekExactWithExists */

    @Test
    void testIndexSeekExactWithExistsByString() throws Exception {
        testIndexSeekExactWithExists("a", "b");
    }

    @Test
    void testIndexSeekExactWithExistsByNumber() throws Exception {
        testIndexSeekExactWithExists(303, 101);
    }

    @Test
    void testIndexSeekExactWithExistsByTemporal() throws Exception {
        testIndexSeekExactWithExists(epochDate(303), epochDate(101));
    }

    @Test
    void testIndexSeekExactWithExistsByBoolean() throws Exception {
        testIndexSeekExactWithExists(true, false);
    }

    @Test
    void testIndexSeekExactWithExistsByStringArray() throws Exception {
        testIndexSeekExactWithExists(new String[] {"a", "c"}, new String[] {"b", "c"});
    }

    @Test
    void testIndexSeekExactWithExistsByNumberArray() throws Exception {
        testIndexSeekExactWithExists(new long[] {303, 900}, new long[] {101, 900});
    }

    @Test
    void testIndexSeekExactWithExistsByBooleanArray() throws Exception {
        testIndexSeekExactWithExists(new boolean[] {true, true}, new boolean[] {false, true});
    }

    @Test
    void testIndexSeekExactWithExistsByTemporalArray() throws Exception {
        testIndexSeekExactWithExists(dateArray(303, 900), dateArray(101, 900));
    }

    @Test
    void testIndexSeekExactWithExistsBySpatial() throws Exception {
        testIndexSeekExactWithExists(pointValue(WGS_84, 100D, 90D), pointValue(WGS_84, 0D, 0D));
    }

    @Test
    void testIndexSeekExactWithExistsBySpatialArray() throws Exception {
        testIndexSeekExactWithExists(
                pointArray(new PointValue[] {pointValue(CARTESIAN, 100D, 100D), pointValue(CARTESIAN, 101D, 101D)}),
                pointArray(new PointValue[] {pointValue(CARTESIAN, 0D, 0D), pointValue(CARTESIAN, 1D, 1D)}));
    }

    private void testIndexSeekExactWithExists(Object a, Object b) throws Exception {
        testIndexSeekExactWithExists(Values.of(a), Values.of(b));
    }

    private void testIndexSeekExactWithExists(Value a, Value b) throws Exception {
        assumeTrue(testSuite.supportsGranularCompositeQueries(), "Assume support for granular composite queries");
        updateAndCommit(asList(
                add(1L, descriptor.schema(), a, Values.of(1)),
                add(2L, descriptor.schema(), b, Values.of("abv")),
                add(3L, descriptor.schema(), a, Values.of(false))));

        assertThat(query(exact(0, a), exists(1))).containsExactly(1L, 3L);
        assertThat(query(exact(0, b), exists(1))).containsExactly(2L);
    }

    /* testIndexSeekRangeWithExists */

    @Test
    void testIndexSeekRangeWithExistsByString() throws Exception {
        testIndexSeekRangeWithExists("Anabelle", "Anna", "Bob", "Harriet", "William");
    }

    @Test
    void testIndexSeekRangeWithExistsByNumber() throws Exception {
        testIndexSeekRangeWithExists(-5, 0, 5.5, 10.0, 100.0);
    }

    @Test
    void testIndexSeekRangeWithExistsByTemporal() throws Exception {
        DateTimeValue d1 = datetime(9999, 100, ZoneId.of("+18:00"));
        DateTimeValue d2 = datetime(10000, 100, ZoneId.of("UTC"));
        DateTimeValue d3 = datetime(10000, 100, ZoneId.of("+01:00"));
        DateTimeValue d4 = datetime(10000, 100, ZoneId.of("Europe/Stockholm"));
        DateTimeValue d5 = datetime(10000, 100, ZoneId.of("+03:00"));
        testIndexSeekRangeWithExists(d1, d2, d3, d4, d5);
    }

    @Test
    void testIndexSeekRangeWithExistsByBoolean() throws Exception {
        assumeTrue(testSuite.supportsGranularCompositeQueries(), "Assume support for granular composite queries");
        assumeTrue(testSuite.supportsBooleanRangeQueries(), "Assume support for boolean range queries");

        updateAndCommit(
                asList(add(1L, descriptor.schema(), false, "someString"), add(2L, descriptor.schema(), true, 1000)));

        assertThat(query(range(0, BooleanValue.FALSE, true, BooleanValue.TRUE, true), exists(1)))
                .containsExactly(1L, 2L);
        assertThat(query(range(0, BooleanValue.FALSE, false, BooleanValue.TRUE, true), exists(1)))
                .containsExactly(2L);
        assertThat(query(range(0, BooleanValue.FALSE, true, BooleanValue.TRUE, false), exists(1)))
                .containsExactly(1L);
        assertThat(query(range(0, BooleanValue.FALSE, false, BooleanValue.TRUE, false), exists(1)))
                .isEmpty();
        assertThat(query(range(0, null, true, BooleanValue.TRUE, true), exists(1)))
                .containsExactly(1L, 2L);
        assertThat(query(range(0, BooleanValue.FALSE, true, null, true), exists(1)))
                .containsExactly(1L, 2L);
        assertThat(query(range(0, BooleanValue.TRUE, true, BooleanValue.FALSE, true), exists(1)))
                .isEmpty();
    }

    @Test
    void testIndexSeekRangeWithExistsByStringArray() throws Exception {
        testIndexSeekRangeWithExists(
                new String[] {"Anabelle", "Anabelle"},
                new String[] {"Anabelle", "Anablo"},
                new String[] {"Anna", "Anabelle"},
                new String[] {"Anna", "Anablo"},
                new String[] {"Bob"});
    }

    @Test
    void testIndexSeekRangeWithExistsByNumberArray() throws Exception {
        testIndexSeekRangeWithExists(
                new long[] {303, 303}, new long[] {303, 404}, new long[] {600, 303}, new long[] {600, 404}, new long[] {
                    900
                });
    }

    @Test
    void testIndexSeekRangeWithExistsByBooleanArray() throws Exception {
        testIndexSeekRangeWithExists(
                new boolean[] {false, false},
                new boolean[] {false, true},
                new boolean[] {true, false},
                new boolean[] {true, true},
                new boolean[] {true, true, false});
    }

    @Test
    void testIndexSeekRangeWithExistsByTemporalArray() throws Exception {
        testIndexSeekRangeWithExists(
                dateArray(303, 303),
                dateArray(303, 404),
                dateArray(404, 303),
                dateArray(404, 404),
                dateArray(404, 404, 303));
    }

    @Test
    void testIndexSeekRangeWithExistsBySpatial() throws Exception {
        testIndexSeekBoundingBoxWithExists(
                pointValue(CARTESIAN, 0D, 0D),
                pointValue(CARTESIAN, 1D, 1D),
                pointValue(CARTESIAN, 2D, 2D),
                pointValue(CARTESIAN, 3D, 3D),
                pointValue(CARTESIAN, 4D, 4D));
    }

    @Test
    void testExactMatchOnRandomCompositeValues() throws Exception {
        // given
        ValueType[] types = randomSetOfSupportedTypes();
        List<ValueIndexEntryUpdate<?>> updates = new ArrayList<>();
        Set<ValueTuple> duplicateChecker = new HashSet<>();
        for (long id = 0; id < 10_000; id++) {
            ValueIndexEntryUpdate<?> update;
            do {
                update = add(
                        id,
                        descriptor.schema(),
                        random.randomValues().nextValueOfTypes(types),
                        random.randomValues().nextValueOfTypes(types));
            } while (!duplicateChecker.add(ValueTuple.of(update.values())));
            updates.add(update);
        }
        updateAndCommit(updates);

        // when
        InMemoryTokens tokenNameLookup = new InMemoryTokens();
        for (ValueIndexEntryUpdate<?> update : updates) {
            // then
            List<Long> hits = query(exact(0, update.values()[0]), exact(1, update.values()[1]));
            assertEquals(1, hits.size(), update.describe(tokenNameLookup) + " " + hits);
            assertThat(single(hits)).isEqualTo(update.getEntityId());
        }
    }

    private void testIndexSeekRangeWithExists(Object obj1, Object obj2, Object obj3, Object obj4, Object obj5)
            throws Exception {
        testIndexSeekRangeWithExists(
                Values.of(obj1), Values.of(obj2), Values.of(obj3), Values.of(obj4), Values.of(obj5));
    }

    private void testIndexSeekRangeWithExists(Value obj1, Value obj2, Value obj3, Value obj4, Value obj5)
            throws Exception {
        assumeTrue(testSuite.supportsGranularCompositeQueries(), "Assume support for granular composite queries");

        updateAndCommit(asList(
                add(1L, descriptor.schema(), obj1, Values.of(100)),
                add(2L, descriptor.schema(), obj2, Values.of("someString")),
                add(3L, descriptor.schema(), obj3, Values.of(epochDate(300))),
                add(4L, descriptor.schema(), obj4, Values.of(true)),
                add(5L, descriptor.schema(), obj5, Values.of(42))));

        assertThat(query(range(0, obj2, true, obj4, false), exists(1))).containsExactly(2L, 3L);
        assertThat(query(range(0, obj4, true, null, false), exists(1))).containsExactly(4L, 5L);
        assertThat(query(range(0, obj4, false, null, true), exists(1))).containsExactly(5L);
        assertThat(query(range(0, obj5, false, obj2, true), exists(1))).isEmpty();
        assertThat(query(range(0, null, false, obj3, false), exists(1))).containsExactly(1L, 2L);
        assertThat(query(range(0, null, true, obj3, true), exists(1))).containsExactly(1L, 2L, 3L);
        assertThat(query(range(0, obj1, false, obj2, true), exists(1))).containsExactly(2L);
        assertThat(query(range(0, obj1, false, obj3, false), exists(1))).containsExactly(2L);
    }

    private void testIndexSeekBoundingBoxWithExists(
            PointValue obj1, PointValue obj2, PointValue obj3, PointValue obj4, PointValue obj5) throws Exception {
        assumeTrue(testSuite.supportsGranularCompositeQueries(), "Assume support for granular composite queries");
        assumeTrue(testSuite.supportsSpatial(), "Assume support for spacial value types");
        assumeTrue(testSuite.supportsBoundingBoxQueries(), "Assume support for bounding box queries");

        updateAndCommit(asList(
                add(1L, descriptor.schema(), obj1, Values.of(100)),
                add(2L, descriptor.schema(), obj2, Values.of("someString")),
                add(3L, descriptor.schema(), obj3, Values.of(epochDate(300))),
                add(4L, descriptor.schema(), obj4, Values.of(true)),
                add(5L, descriptor.schema(), obj5, Values.of(42))));

        assertThat(query(boundingBox(0, obj2, obj4), exists(1))).containsExactly(2L, 3L, 4L);
        assertThat(query(boundingBox(0, obj5, obj2), exists(1))).isEmpty();
        assertThat(query(boundingBox(0, obj1, obj2), exists(1))).containsExactly(1L, 2L);
        assertThat(query(boundingBox(0, obj1, obj3), exists(1))).containsExactly(1L, 2L, 3L);
    }

    /* IndexOrder */

    @Test
    void shouldRangeSeekInOrderNumberAscending() throws Exception {
        Object o0 = 0;
        Object o1 = 1;
        Object o2 = 2;
        Object o3 = 3;
        Object o4 = 4;
        Object o5 = 5;
        shouldSeekInOrderExactWithRange(IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderNumberDescending() throws Exception {
        Object o0 = 0;
        Object o1 = 1;
        Object o2 = 2;
        Object o3 = 3;
        Object o4 = 4;
        Object o5 = 5;
        shouldSeekInOrderExactWithRange(IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderStringAscending() throws Exception {
        Object o0 = "0";
        Object o1 = "1";
        Object o2 = "2";
        Object o3 = "3";
        Object o4 = "4";
        Object o5 = "5";
        shouldSeekInOrderExactWithRange(IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderStringDescending() throws Exception {
        Object o0 = "0";
        Object o1 = "1";
        Object o2 = "2";
        Object o3 = "3";
        Object o4 = "4";
        Object o5 = "5";
        shouldSeekInOrderExactWithRange(IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderAscendingDate() throws Exception {
        Object o0 = DateValue.epochDateRaw(0);
        Object o1 = DateValue.epochDateRaw(1);
        Object o2 = DateValue.epochDateRaw(2);
        Object o3 = DateValue.epochDateRaw(3);
        Object o4 = DateValue.epochDateRaw(4);
        Object o5 = DateValue.epochDateRaw(5);
        shouldSeekInOrderExactWithRange(IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderDescendingDate() throws Exception {
        Object o0 = DateValue.epochDateRaw(0);
        Object o1 = DateValue.epochDateRaw(1);
        Object o2 = DateValue.epochDateRaw(2);
        Object o3 = DateValue.epochDateRaw(3);
        Object o4 = DateValue.epochDateRaw(4);
        Object o5 = DateValue.epochDateRaw(5);
        shouldSeekInOrderExactWithRange(IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderAscendingLocalTime() throws Exception {
        Object o0 = LocalTimeValue.localTimeRaw(0);
        Object o1 = LocalTimeValue.localTimeRaw(1);
        Object o2 = LocalTimeValue.localTimeRaw(2);
        Object o3 = LocalTimeValue.localTimeRaw(3);
        Object o4 = LocalTimeValue.localTimeRaw(4);
        Object o5 = LocalTimeValue.localTimeRaw(5);
        shouldSeekInOrderExactWithRange(IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderDescendingLocalTime() throws Exception {
        Object o0 = LocalTimeValue.localTimeRaw(0);
        Object o1 = LocalTimeValue.localTimeRaw(1);
        Object o2 = LocalTimeValue.localTimeRaw(2);
        Object o3 = LocalTimeValue.localTimeRaw(3);
        Object o4 = LocalTimeValue.localTimeRaw(4);
        Object o5 = LocalTimeValue.localTimeRaw(5);
        shouldSeekInOrderExactWithRange(IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderAscendingTime() throws Exception {
        Object o0 = TimeValue.timeRaw(0, ZoneOffset.ofHours(0));
        Object o1 = TimeValue.timeRaw(1, ZoneOffset.ofHours(0));
        Object o2 = TimeValue.timeRaw(2, ZoneOffset.ofHours(0));
        Object o3 = TimeValue.timeRaw(3, ZoneOffset.ofHours(0));
        Object o4 = TimeValue.timeRaw(4, ZoneOffset.ofHours(0));
        Object o5 = TimeValue.timeRaw(5, ZoneOffset.ofHours(0));
        shouldSeekInOrderExactWithRange(IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderDescendingTime() throws Exception {
        Object o0 = TimeValue.timeRaw(0, ZoneOffset.ofHours(0));
        Object o1 = TimeValue.timeRaw(1, ZoneOffset.ofHours(0));
        Object o2 = TimeValue.timeRaw(2, ZoneOffset.ofHours(0));
        Object o3 = TimeValue.timeRaw(3, ZoneOffset.ofHours(0));
        Object o4 = TimeValue.timeRaw(4, ZoneOffset.ofHours(0));
        Object o5 = TimeValue.timeRaw(5, ZoneOffset.ofHours(0));
        shouldSeekInOrderExactWithRange(IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderAscendingLocalDateTime() throws Exception {
        Object o0 = LocalDateTimeValue.localDateTimeRaw(10, 0);
        Object o1 = LocalDateTimeValue.localDateTimeRaw(10, 1);
        Object o2 = LocalDateTimeValue.localDateTimeRaw(10, 2);
        Object o3 = LocalDateTimeValue.localDateTimeRaw(10, 3);
        Object o4 = LocalDateTimeValue.localDateTimeRaw(10, 4);
        Object o5 = LocalDateTimeValue.localDateTimeRaw(10, 5);
        shouldSeekInOrderExactWithRange(IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderDescendingLocalDateTime() throws Exception {
        Object o0 = LocalDateTimeValue.localDateTimeRaw(10, 0);
        Object o1 = LocalDateTimeValue.localDateTimeRaw(10, 1);
        Object o2 = LocalDateTimeValue.localDateTimeRaw(10, 2);
        Object o3 = LocalDateTimeValue.localDateTimeRaw(10, 3);
        Object o4 = LocalDateTimeValue.localDateTimeRaw(10, 4);
        Object o5 = LocalDateTimeValue.localDateTimeRaw(10, 5);
        shouldSeekInOrderExactWithRange(IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderAscendingDateTime() throws Exception {
        Object o0 = DateTimeValue.datetimeRaw(1, 0, ZoneId.of("UTC"));
        Object o1 = DateTimeValue.datetimeRaw(1, 1, ZoneId.of("UTC"));
        Object o2 = DateTimeValue.datetimeRaw(1, 2, ZoneId.of("UTC"));
        Object o3 = DateTimeValue.datetimeRaw(1, 3, ZoneId.of("UTC"));
        Object o4 = DateTimeValue.datetimeRaw(1, 4, ZoneId.of("UTC"));
        Object o5 = DateTimeValue.datetimeRaw(1, 5, ZoneId.of("UTC"));
        shouldSeekInOrderExactWithRange(IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderDescendingDateTime() throws Exception {
        Object o0 = DateTimeValue.datetimeRaw(1, 0, ZoneId.of("UTC"));
        Object o1 = DateTimeValue.datetimeRaw(1, 1, ZoneId.of("UTC"));
        Object o2 = DateTimeValue.datetimeRaw(1, 2, ZoneId.of("UTC"));
        Object o3 = DateTimeValue.datetimeRaw(1, 3, ZoneId.of("UTC"));
        Object o4 = DateTimeValue.datetimeRaw(1, 4, ZoneId.of("UTC"));
        Object o5 = DateTimeValue.datetimeRaw(1, 5, ZoneId.of("UTC"));
        shouldSeekInOrderExactWithRange(IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderAscendingNumberArray() throws Exception {
        Object o0 = new int[] {0};
        Object o1 = new int[] {1};
        Object o2 = new int[] {2};
        Object o3 = new int[] {3};
        Object o4 = new int[] {4};
        Object o5 = new int[] {5};
        shouldSeekInOrderExactWithRange(IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderDescendingNumberArray() throws Exception {
        Object o0 = new int[] {0};
        Object o1 = new int[] {1};
        Object o2 = new int[] {2};
        Object o3 = new int[] {3};
        Object o4 = new int[] {4};
        Object o5 = new int[] {5};
        shouldSeekInOrderExactWithRange(IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderAscendingStringArray() throws Exception {
        Object o0 = new String[] {"0"};
        Object o1 = new String[] {"1"};
        Object o2 = new String[] {"2"};
        Object o3 = new String[] {"3"};
        Object o4 = new String[] {"4"};
        Object o5 = new String[] {"5"};
        shouldSeekInOrderExactWithRange(IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderDescendingStringArray() throws Exception {
        Object o0 = new String[] {"0"};
        Object o1 = new String[] {"1"};
        Object o2 = new String[] {"2"};
        Object o3 = new String[] {"3"};
        Object o4 = new String[] {"4"};
        Object o5 = new String[] {"5"};
        shouldSeekInOrderExactWithRange(IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderAscendingBooleanArray() throws Exception {
        Object o0 = new boolean[] {false};
        Object o1 = new boolean[] {false, false};
        Object o2 = new boolean[] {false, true};
        Object o3 = new boolean[] {true};
        Object o4 = new boolean[] {true, false};
        Object o5 = new boolean[] {true, true};
        shouldSeekInOrderExactWithRange(IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderDescendingBooleanArray() throws Exception {
        Object o0 = new boolean[] {false};
        Object o1 = new boolean[] {false, false};
        Object o2 = new boolean[] {false, true};
        Object o3 = new boolean[] {true};
        Object o4 = new boolean[] {true, false};
        Object o5 = new boolean[] {true, true};
        shouldSeekInOrderExactWithRange(IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderAscendingDateTimeArray() throws Exception {
        Object o0 = new ZonedDateTime[] {ZonedDateTime.of(10, 10, 10, 10, 10, 10, 0, ZoneId.of("UTC"))};
        Object o1 = new ZonedDateTime[] {ZonedDateTime.of(10, 10, 10, 10, 10, 10, 1, ZoneId.of("UTC"))};
        Object o2 = new ZonedDateTime[] {ZonedDateTime.of(10, 10, 10, 10, 10, 10, 2, ZoneId.of("UTC"))};
        Object o3 = new ZonedDateTime[] {ZonedDateTime.of(10, 10, 10, 10, 10, 10, 3, ZoneId.of("UTC"))};
        Object o4 = new ZonedDateTime[] {ZonedDateTime.of(10, 10, 10, 10, 10, 10, 4, ZoneId.of("UTC"))};
        Object o5 = new ZonedDateTime[] {ZonedDateTime.of(10, 10, 10, 10, 10, 10, 5, ZoneId.of("UTC"))};
        shouldSeekInOrderExactWithRange(IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderDescendingDateTimeArray() throws Exception {
        Object o0 = new ZonedDateTime[] {ZonedDateTime.of(10, 10, 10, 10, 10, 10, 0, ZoneId.of("UTC"))};
        Object o1 = new ZonedDateTime[] {ZonedDateTime.of(10, 10, 10, 10, 10, 10, 1, ZoneId.of("UTC"))};
        Object o2 = new ZonedDateTime[] {ZonedDateTime.of(10, 10, 10, 10, 10, 10, 2, ZoneId.of("UTC"))};
        Object o3 = new ZonedDateTime[] {ZonedDateTime.of(10, 10, 10, 10, 10, 10, 3, ZoneId.of("UTC"))};
        Object o4 = new ZonedDateTime[] {ZonedDateTime.of(10, 10, 10, 10, 10, 10, 4, ZoneId.of("UTC"))};
        Object o5 = new ZonedDateTime[] {ZonedDateTime.of(10, 10, 10, 10, 10, 10, 5, ZoneId.of("UTC"))};
        shouldSeekInOrderExactWithRange(IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderAscendingLocalDateTimeArray() throws Exception {
        Object o0 = new LocalDateTime[] {LocalDateTime.of(10, 10, 10, 10, 10, 10, 0)};
        Object o1 = new LocalDateTime[] {LocalDateTime.of(10, 10, 10, 10, 10, 10, 1)};
        Object o2 = new LocalDateTime[] {LocalDateTime.of(10, 10, 10, 10, 10, 10, 2)};
        Object o3 = new LocalDateTime[] {LocalDateTime.of(10, 10, 10, 10, 10, 10, 3)};
        Object o4 = new LocalDateTime[] {LocalDateTime.of(10, 10, 10, 10, 10, 10, 4)};
        Object o5 = new LocalDateTime[] {LocalDateTime.of(10, 10, 10, 10, 10, 10, 5)};
        shouldSeekInOrderExactWithRange(IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderDescendingLocalDateTimeArray() throws Exception {
        Object o0 = new LocalDateTime[] {LocalDateTime.of(10, 10, 10, 10, 10, 10, 0)};
        Object o1 = new LocalDateTime[] {LocalDateTime.of(10, 10, 10, 10, 10, 10, 1)};
        Object o2 = new LocalDateTime[] {LocalDateTime.of(10, 10, 10, 10, 10, 10, 2)};
        Object o3 = new LocalDateTime[] {LocalDateTime.of(10, 10, 10, 10, 10, 10, 3)};
        Object o4 = new LocalDateTime[] {LocalDateTime.of(10, 10, 10, 10, 10, 10, 4)};
        Object o5 = new LocalDateTime[] {LocalDateTime.of(10, 10, 10, 10, 10, 10, 5)};
        shouldSeekInOrderExactWithRange(IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderAscendingTimeArray() throws Exception {
        Object o0 = new OffsetTime[] {OffsetTime.of(10, 10, 10, 0, ZoneOffset.ofHours(0))};
        Object o1 = new OffsetTime[] {OffsetTime.of(10, 10, 10, 1, ZoneOffset.ofHours(0))};
        Object o2 = new OffsetTime[] {OffsetTime.of(10, 10, 10, 2, ZoneOffset.ofHours(0))};
        Object o3 = new OffsetTime[] {OffsetTime.of(10, 10, 10, 3, ZoneOffset.ofHours(0))};
        Object o4 = new OffsetTime[] {OffsetTime.of(10, 10, 10, 4, ZoneOffset.ofHours(0))};
        Object o5 = new OffsetTime[] {OffsetTime.of(10, 10, 10, 5, ZoneOffset.ofHours(0))};
        shouldSeekInOrderExactWithRange(IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderDescendingTimeArray() throws Exception {
        Object o0 = new OffsetTime[] {OffsetTime.of(10, 10, 10, 0, ZoneOffset.ofHours(0))};
        Object o1 = new OffsetTime[] {OffsetTime.of(10, 10, 10, 1, ZoneOffset.ofHours(0))};
        Object o2 = new OffsetTime[] {OffsetTime.of(10, 10, 10, 2, ZoneOffset.ofHours(0))};
        Object o3 = new OffsetTime[] {OffsetTime.of(10, 10, 10, 3, ZoneOffset.ofHours(0))};
        Object o4 = new OffsetTime[] {OffsetTime.of(10, 10, 10, 4, ZoneOffset.ofHours(0))};
        Object o5 = new OffsetTime[] {OffsetTime.of(10, 10, 10, 5, ZoneOffset.ofHours(0))};
        shouldSeekInOrderExactWithRange(IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderAscendingDateArray() throws Exception {
        Object o0 = new LocalDate[] {LocalDate.of(10, 10, 1)};
        Object o1 = new LocalDate[] {LocalDate.of(10, 10, 2)};
        Object o2 = new LocalDate[] {LocalDate.of(10, 10, 3)};
        Object o3 = new LocalDate[] {LocalDate.of(10, 10, 4)};
        Object o4 = new LocalDate[] {LocalDate.of(10, 10, 5)};
        Object o5 = new LocalDate[] {LocalDate.of(10, 10, 6)};
        shouldSeekInOrderExactWithRange(IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderDescendingDateArray() throws Exception {
        Object o0 = new LocalDate[] {LocalDate.of(10, 10, 1)};
        Object o1 = new LocalDate[] {LocalDate.of(10, 10, 2)};
        Object o2 = new LocalDate[] {LocalDate.of(10, 10, 3)};
        Object o3 = new LocalDate[] {LocalDate.of(10, 10, 4)};
        Object o4 = new LocalDate[] {LocalDate.of(10, 10, 5)};
        Object o5 = new LocalDate[] {LocalDate.of(10, 10, 6)};
        shouldSeekInOrderExactWithRange(IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderAscendingLocalTimeArray() throws Exception {
        Object o0 = new LocalTime[] {LocalTime.of(10, 0)};
        Object o1 = new LocalTime[] {LocalTime.of(10, 1)};
        Object o2 = new LocalTime[] {LocalTime.of(10, 2)};
        Object o3 = new LocalTime[] {LocalTime.of(10, 3)};
        Object o4 = new LocalTime[] {LocalTime.of(10, 4)};
        Object o5 = new LocalTime[] {LocalTime.of(10, 5)};
        shouldSeekInOrderExactWithRange(IndexOrder.ASCENDING, o0, o1, o2, o3, o4, o5);
    }

    @Test
    void shouldRangeSeekInOrderDescendingLocalTimeArray() throws Exception {
        Object o0 = new LocalTime[] {LocalTime.of(10, 0)};
        Object o1 = new LocalTime[] {LocalTime.of(10, 1)};
        Object o2 = new LocalTime[] {LocalTime.of(10, 2)};
        Object o3 = new LocalTime[] {LocalTime.of(10, 3)};
        Object o4 = new LocalTime[] {LocalTime.of(10, 4)};
        Object o5 = new LocalTime[] {LocalTime.of(10, 5)};
        shouldSeekInOrderExactWithRange(IndexOrder.DESCENDING, o0, o1, o2, o3, o4, o5);
    }

    private void shouldSeekInOrderExactWithRange(
            IndexOrder order, Object o0, Object o1, Object o2, Object o3, Object o4, Object o5) throws Exception {
        Object baseValue = 1; // Todo use random value instead
        PropertyIndexQuery exact = exact(100, baseValue);
        PropertyIndexQuery range = range(200, Values.of(o0), true, Values.of(o5), true);
        if (order == IndexOrder.ASCENDING || order == IndexOrder.DESCENDING) {
            assumeTrue(descriptor.getCapability().supportsOrdering(), "Assume support for order " + order);
        }

        updateAndCommit(asList(
                add(1, descriptor.schema(), baseValue, o0),
                add(1, descriptor.schema(), baseValue, o5),
                add(1, descriptor.schema(), baseValue, o1),
                add(1, descriptor.schema(), baseValue, o4),
                add(1, descriptor.schema(), baseValue, o2),
                add(1, descriptor.schema(), baseValue, o3)));

        SimpleEntityValueClient client = new SimpleEntityValueClient();
        try (AutoCloseable ignored = query(client, order, exact, range)) {
            List<Long> seenIds = assertClientReturnValuesInOrder(client, order);
            assertThat(seenIds.size()).isEqualTo(6);
        }
    }

    /* Composite query validity */

    /**
     * This test verify behavior for all different index patterns on a two column composite index.
     * A composite query need to have decreasing precision among the queries.
     * This means a range or exists query can only be followed by and exists query.
     * Prefix query is also included under "range".
     * Contains or suffix queries are not allowed in a composite query at all.
     *
     * Those are all the different combinations:
     * a = allEntries
     * - = exists
     * x = exact
     * < = range (also include stringPrefix)
     * ! = stringContains or stringSuffix
     * ? = any predicate
     * Index patterns
     * x x ok
     * x < ok
     * x - ok
     * < x not ok
     * < < not ok
     * < - ok
     * - x not ok
     * - < not ok
     * - - ok
     * ! ? not ok
     * a ? not ok
     * ? a not ok
     */
    @Test
    void mustThrowOnIllegalCompositeQueriesAndMustNotThrowOnLegalQueries() throws Exception {
        assumeTrue(testSuite.supportsGranularCompositeQueries(), "Assume support for granular composite queries");

        // given
        Value someValue = Values.of(true);
        TextValue someString = stringValue("");
        PropertyIndexQuery allEntries = allEntries();
        PropertyIndexQuery firstExact = exact(100, someValue);
        PropertyIndexQuery firstRange = range(100, someValue, true, someValue, true);
        PropertyIndexQuery firstPrefix = stringPrefix(100, someString);
        PropertyIndexQuery firstExist = exists(100);
        PropertyIndexQuery firstSuffix = stringSuffix(100, someString);
        PropertyIndexQuery firstContains = stringContains(100, someString);
        PropertyIndexQuery secondExact = exact(200, someValue);
        PropertyIndexQuery secondRange = range(200, someValue, true, someValue, true);
        PropertyIndexQuery secondExist = exists(200);
        PropertyIndexQuery secondPrefix = stringPrefix(100, someString);
        PropertyIndexQuery secondSuffix = stringSuffix(100, someString);
        PropertyIndexQuery secondContains = stringContains(100, someString);

        List<Pair<PropertyIndexQuery[], Boolean>> queries = Arrays.asList(
                of(new PropertyIndexQuery[] {allEntries, allEntries}, false),
                of(new PropertyIndexQuery[] {allEntries, secondExist}, false),
                of(new PropertyIndexQuery[] {allEntries, secondExact}, false),
                of(new PropertyIndexQuery[] {allEntries, secondRange}, false),
                of(new PropertyIndexQuery[] {allEntries, secondPrefix}, false),
                of(new PropertyIndexQuery[] {allEntries, secondSuffix}, false),
                of(new PropertyIndexQuery[] {allEntries, secondContains}, false),
                of(new PropertyIndexQuery[] {firstExist, allEntries}, false),
                of(new PropertyIndexQuery[] {firstExist, secondExist}, true),
                of(new PropertyIndexQuery[] {firstExist, secondExact}, false),
                of(new PropertyIndexQuery[] {firstExist, secondRange}, false),
                of(new PropertyIndexQuery[] {firstExist, secondPrefix}, false),
                of(new PropertyIndexQuery[] {firstExist, secondSuffix}, false),
                of(new PropertyIndexQuery[] {firstExist, secondContains}, false),
                of(new PropertyIndexQuery[] {firstExact, allEntries}, false),
                of(new PropertyIndexQuery[] {firstExact, secondExist}, true),
                of(new PropertyIndexQuery[] {firstExact, secondExact}, true),
                of(new PropertyIndexQuery[] {firstExact, secondRange}, true),
                of(new PropertyIndexQuery[] {firstExact, secondPrefix}, true),
                of(new PropertyIndexQuery[] {firstExact, secondSuffix}, false),
                of(new PropertyIndexQuery[] {firstExact, secondContains}, false),
                of(new PropertyIndexQuery[] {firstRange, allEntries}, false),
                of(new PropertyIndexQuery[] {firstRange, secondExist}, true),
                of(new PropertyIndexQuery[] {firstRange, secondExact}, false),
                of(new PropertyIndexQuery[] {firstRange, secondRange}, false),
                of(new PropertyIndexQuery[] {firstRange, secondPrefix}, false),
                of(new PropertyIndexQuery[] {firstRange, secondSuffix}, false),
                of(new PropertyIndexQuery[] {firstRange, secondContains}, false),
                of(new PropertyIndexQuery[] {firstPrefix, allEntries}, false),
                of(new PropertyIndexQuery[] {firstPrefix, secondExist}, true),
                of(new PropertyIndexQuery[] {firstPrefix, secondExact}, false),
                of(new PropertyIndexQuery[] {firstPrefix, secondRange}, false),
                of(new PropertyIndexQuery[] {firstPrefix, secondPrefix}, false),
                of(new PropertyIndexQuery[] {firstPrefix, secondSuffix}, false),
                of(new PropertyIndexQuery[] {firstPrefix, secondContains}, false),
                of(new PropertyIndexQuery[] {firstSuffix, allEntries}, false),
                of(new PropertyIndexQuery[] {firstSuffix, secondExist}, false),
                of(new PropertyIndexQuery[] {firstSuffix, secondExact}, false),
                of(new PropertyIndexQuery[] {firstSuffix, secondRange}, false),
                of(new PropertyIndexQuery[] {firstSuffix, secondPrefix}, false),
                of(new PropertyIndexQuery[] {firstSuffix, secondSuffix}, false),
                of(new PropertyIndexQuery[] {firstSuffix, secondContains}, false),
                of(new PropertyIndexQuery[] {firstContains, allEntries}, false),
                of(new PropertyIndexQuery[] {firstContains, secondExist}, false),
                of(new PropertyIndexQuery[] {firstContains, secondExact}, false),
                of(new PropertyIndexQuery[] {firstContains, secondRange}, false),
                of(new PropertyIndexQuery[] {firstContains, secondPrefix}, false),
                of(new PropertyIndexQuery[] {firstContains, secondSuffix}, false),
                of(new PropertyIndexQuery[] {firstContains, secondContains}, false));

        SimpleEntityValueClient client = new SimpleEntityValueClient();
        try (ValueIndexReader reader = accessor.newValueReader(NO_USAGE_TRACKING)) {
            for (Pair<PropertyIndexQuery[], Boolean> pair : queries) {
                PropertyIndexQuery[] theQuery = pair.first();
                Boolean legal = pair.other();
                if (legal) {
                    // when
                    reader.query(client, NULL_CONTEXT, unconstrained(), theQuery);

                    // then should not throw
                } else {
                    try {
                        // when
                        reader.query(client, NULL_CONTEXT, unconstrained(), theQuery);
                        fail("Expected index reader to throw for illegal composite query. Query was, "
                                + Arrays.toString(theQuery));
                    } catch (IllegalArgumentException e) {
                        // then
                        if (!testSuite.supportsContainsAndEndsWithQueries() && hasContainsOrEndsWithQuery(theQuery)) {
                            assertThat(e.getMessage()).contains("Tried to query index with illegal query.");
                        } else {
                            assertThat(e.getMessage()).contains("Tried to query index with illegal composite query.");
                        }
                    }
                }
            }
        }
    }

    private boolean hasContainsOrEndsWithQuery(PropertyIndexQuery... query) {
        for (final var predicate : query) {
            switch (predicate.type()) {
                case STRING_CONTAINS, STRING_SUFFIX:
                    return true;
                default:
                    break;
            }
        }
        return false;
    }

    @Test
    void shouldUpdateEntries() throws Exception {
        ValueType[] valueTypes = testSuite.supportedValueTypes();
        long entityId = random.nextLong(1_000_000_000);
        for (ValueType valueType : valueTypes) {
            // given
            Value[] value = new Value[] {random.nextValue(valueType), random.nextValue(valueType)};
            updateAndCommit(singletonList(IndexEntryUpdate.add(entityId, descriptor, value)));
            assertEquals(singletonList(entityId), query(exactQuery(value)));

            // when
            Value[] newValue;
            do {
                newValue = new Value[] {random.nextValue(valueType), random.nextValue(valueType)};
            } while (ValueTuple.of(value).equals(ValueTuple.of(newValue)));
            updateAndCommit(singletonList(IndexEntryUpdate.change(entityId, descriptor, value, newValue)));

            // then
            assertEquals(emptyList(), query(exactQuery(value)));
            assertEquals(singletonList(entityId), query(exactQuery(newValue)));
        }
    }

    @Test
    void shouldRemoveEntries() throws Exception {
        ValueType[] valueTypes = testSuite.supportedValueTypes();
        long entityId = random.nextLong(1_000_000_000);
        for (ValueType valueType : valueTypes) {
            // given
            Value[] value = new Value[] {random.nextValue(valueType), random.nextValue(valueType)};
            updateAndCommit(singletonList(IndexEntryUpdate.add(entityId, descriptor, value)));
            assertEquals(singletonList(entityId), query(exactQuery(value)));

            // when
            updateAndCommit(singletonList(IndexEntryUpdate.remove(entityId, descriptor, value)));

            // then
            assertEquals(emptyList(), query(exactQuery(value)));
        }
    }

    private static PropertyIndexQuery[] exactQuery(Value[] values) {
        return Stream.of(values).map(v -> exact(0, v)).toArray(PropertyIndexQuery[]::new);
    }

    // This behaviour is expected by General indexes
    abstract static class General extends CompositeIndexAccessorCompatibility {
        General(PropertyIndexProviderCompatibilityTestSuite testSuite) {
            super(testSuite, IndexPrototype.forSchema(forLabel(1000, 100, 200)));
        }

        @Test
        void testDuplicatesInIndexSeekByString() throws Exception {
            Object value = "a";
            testDuplicatesInIndexSeek(value);
        }

        @Test
        void testDuplicatesInIndexSeekByNumber() throws Exception {
            testDuplicatesInIndexSeek(333);
        }

        @Test
        void testDuplicatesInIndexSeekByPoint() throws Exception {
            assumeTrue(testSuite.supportsSpatial(), "Assume support for spatial");
            testDuplicatesInIndexSeek(pointValue(WGS_84, 12.6, 56.7));
        }

        @Test
        void testDuplicatesInIndexSeekByBoolean() throws Exception {
            testDuplicatesInIndexSeek(true);
        }

        @Test
        void testDuplicatesInIndexSeekByTemporal() throws Exception {
            testDuplicatesInIndexSeek(ofEpochDay(303));
        }

        @Test
        void testDuplicatesInIndexSeekByStringArray() throws Exception {
            testDuplicatesInIndexSeek(new String[] {"anabelle", "anabollo"});
        }

        @Test
        void testDuplicatesInIndexSeekByNumberArray() throws Exception {
            testDuplicatesInIndexSeek(new long[] {303, 606});
        }

        @Test
        void testDuplicatesInIndexSeekByBooleanArray() throws Exception {
            testDuplicatesInIndexSeek(new boolean[] {true, false});
        }

        @Test
        void testDuplicatesInIndexSeekByTemporalArray() throws Exception {
            testDuplicatesInIndexSeek(dateArray(303, 606));
        }

        @Test
        void testDuplicatesInIndexSeekByPointArray() throws Exception {
            assumeTrue(testSuite.supportsSpatial(), "Assume support for spatial");
            testDuplicatesInIndexSeek(
                    pointArray(new PointValue[] {pointValue(WGS_84, 12.6, 56.7), pointValue(WGS_84, 12.6, 56.7)}));
        }

        private void testDuplicatesInIndexSeek(Object value) throws Exception {
            testDuplicatesInIndexSeek(Values.of(value));
        }

        private void testDuplicatesInIndexSeek(Value value) throws Exception {
            updateAndCommit(
                    asList(add(1L, descriptor.schema(), value, value), add(2L, descriptor.schema(), value, value)));

            assertThat(query(exact(0, value), exact(1, value))).containsExactly(1L, 2L);
        }
    }

    // This behaviour is expected by Unique indexes
    abstract static class Unique extends CompositeIndexAccessorCompatibility {
        Unique(PropertyIndexProviderCompatibilityTestSuite testSuite) {
            super(testSuite, IndexPrototype.uniqueForSchema(forLabel(1000, 100, 200)));
        }

        @Test
        void closingAnOnlineIndexUpdaterMustNotThrowEvenIfItHasBeenFedConflictingData() throws Exception {
            // The reason is that we use and close IndexUpdaters in commit - not in prepare - and therefor
            // we cannot have them go around and throw exceptions, because that could potentially break
            // recovery.
            // Conflicting data can happen because of faulty data coercion. These faults are resolved by
            // the exact-match filtering we do on index seeks.

            updateAndCommit(asList(add(1L, descriptor.schema(), "a", "a"), add(2L, descriptor.schema(), "a", "a")));

            assertThat(query(exact(0, "a"), exact(1, "a"))).containsExactly(1L, 2L);
        }
    }

    private static ArrayValue dateArray(int... epochDays) {
        LocalDate[] localDates = new LocalDate[epochDays.length];
        for (int i = 0; i < epochDays.length; i++) {
            localDates[i] = ofEpochDay(epochDays[i]);
        }
        return Values.dateArray(localDates);
    }

    private static ValueIndexEntryUpdate<SchemaDescriptorSupplier> add(
            long nodeId, SchemaDescriptor schema, Object value1, Object value2) {
        return add(nodeId, schema, Values.of(value1), Values.of(value2));
    }

    private static ValueIndexEntryUpdate<SchemaDescriptorSupplier> add(
            long nodeId, SchemaDescriptor schema, Value value1, Value value2) {
        return IndexEntryUpdate.add(nodeId, () -> schema, value1, value2);
    }
}
