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
package org.neo4j.kernel.impl.newapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.neo4j.collection.diffset.TrackableDiffSets.newMutableLongDiffSets;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesForBoundingBoxSeek;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesForRangeSeek;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesForRangeSeekByPrefix;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesForScan;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesForSeek;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesForSuffixOrContains;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesWithValuesForBoundingBoxSeek;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesWithValuesForRangeSeek;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesWithValuesForRangeSeekByPrefix;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesWithValuesForScan;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesWithValuesForSuffixOrContains;
import static org.neo4j.values.storable.CoordinateReferenceSystem.CARTESIAN;
import static org.neo4j.values.storable.CoordinateReferenceSystem.CARTESIAN_3D;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS_84;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS_84_3D;
import static org.neo4j.values.storable.PointValue.maxPointValueOf;
import static org.neo4j.values.storable.PointValue.minPointValueOf;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.intArray;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.pointValue;
import static org.neo4j.values.storable.Values.stringValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.lang3.ArrayUtils;
import org.eclipse.collections.api.LongIterable;
import org.eclipse.collections.impl.UnmodifiableMap;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.mockito.Mockito;
import org.neo4j.collection.diffset.MutableLongDiffSets;
import org.neo4j.collection.factory.OnHeapCollectionsFactory;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.kernel.impl.newapi.TxStateIndexChanges.AddedAndRemoved;
import org.neo4j.kernel.impl.newapi.TxStateIndexChanges.AddedWithValuesAndRemoved;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueTuple;
import org.neo4j.values.storable.Values;

class TxStateIndexChangesTest {
    private final IndexDescriptor index = TestIndexDescriptorFactory.forLabel(1, 1);

    @Test
    void shouldComputeIndexUpdatesForScanOnAnEmptyTxState() {
        final ReadableTransactionState state = Mockito.mock(ReadableTransactionState.class);

        // WHEN
        AddedAndRemoved changes = indexUpdatesForScan(state, index, IndexOrder.NONE);
        AddedWithValuesAndRemoved changesWithValues = indexUpdatesWithValuesForScan(state, index, IndexOrder.NONE);

        // THEN
        assertTrue(changes.isEmpty());
        assertTrue(changesWithValues.isEmpty());
    }

    @Test
    void shouldComputeIndexUpdatesForScanWhenThereAreNewEntities() {
        // GIVEN
        final ReadableTransactionState state =
                new TxStateBuilder().withAdded(42L, "foo").withAdded(43L, "bar").build();

        // WHEN
        AddedAndRemoved changes = indexUpdatesForScan(state, index, IndexOrder.NONE);
        AddedWithValuesAndRemoved changesWithValues = indexUpdatesWithValuesForScan(state, index, IndexOrder.NONE);

        // THEN
        assertContains(changes.added(), 42L, 43L);
        assertContains(
                changesWithValues.added(), entityWithPropertyValues(42L, "foo"), entityWithPropertyValues(43L, "bar"));
    }

    @Test
    void shouldComputeIndexUpdatesForScan() {
        assertScanWithOrder(IndexOrder.NONE);
        assertScanWithOrder(IndexOrder.ASCENDING);
    }

    @Test
    void shouldComputeIndexUpdatesForScanWithDescendingOrder() {
        assertScanWithOrder(IndexOrder.DESCENDING);
    }

    private void assertScanWithOrder(IndexOrder indexOrder) {
        // GIVEN
        final ReadableTransactionState state = new TxStateBuilder()
                .withAdded(40L, "Aaron")
                .withAdded(41L, "Agatha")
                .withAdded(42L, "Andreas")
                .withAdded(43L, "Barbarella")
                .withAdded(44L, "Andrea")
                .withAdded(45L, "Aristotle")
                .withAdded(46L, "Barbara")
                .withAdded(47L, "Cinderella")
                .build();

        // WHEN
        AddedAndRemoved changes = indexUpdatesForScan(state, index, indexOrder);
        AddedWithValuesAndRemoved changesWithValues = indexUpdatesWithValuesForScan(state, index, indexOrder);

        EntityWithPropertyValues[] expectedEntitiesWithValues = {
            entityWithPropertyValues(40L, "Aaron"),
            entityWithPropertyValues(41L, "Agatha"),
            entityWithPropertyValues(44L, "Andrea"),
            entityWithPropertyValues(42L, "Andreas"),
            entityWithPropertyValues(45L, "Aristotle"),
            entityWithPropertyValues(46L, "Barbara"),
            entityWithPropertyValues(43L, "Barbarella"),
            entityWithPropertyValues(47L, "Cinderella")
        };

        // THEN
        assertContains(indexOrder, changes, changesWithValues, expectedEntitiesWithValues);
    }

    @Test
    void shouldComputeIndexUpdatesForSeekWhenThereAreNewEntities() {
        // GIVEN
        final ReadableTransactionState state =
                new TxStateBuilder().withAdded(42L, "foo").withAdded(43L, "bar").build();

        // WHEN
        AddedAndRemoved changes = indexUpdatesForSeek(state, index, ValueTuple.of("bar"));

        // THEN
        assertContains(changes.added(), 43L);
    }

    @TestFactory
    Collection<DynamicTest> rangeTests() {
        final var state = new TxStateBuilder()
                .withAdded(42L, 510)
                .withAdded(43L, 520)
                .withAdded(44L, 550)
                .withAdded(45L, 500)
                .withAdded(46L, 530)
                .withAdded(47L, 560)
                .withAdded(48L, 540)
                .build();

        final var tests = new ArrayList<DynamicTest>();

        tests.addAll(rangeTest(
                state,
                Values.of(510),
                true,
                Values.of(550),
                true,
                entityWithPropertyValues(42L, 510),
                entityWithPropertyValues(43L, 520),
                entityWithPropertyValues(46L, 530),
                entityWithPropertyValues(48L, 540),
                entityWithPropertyValues(44L, 550)));
        tests.addAll(rangeTest(
                state,
                Values.of(510),
                true,
                Values.of(550),
                false,
                entityWithPropertyValues(42L, 510),
                entityWithPropertyValues(43L, 520),
                entityWithPropertyValues(46L, 530),
                entityWithPropertyValues(48L, 540)));
        tests.addAll(rangeTest(
                state,
                Values.of(510),
                false,
                Values.of(550),
                true,
                entityWithPropertyValues(43L, 520),
                entityWithPropertyValues(46L, 530),
                entityWithPropertyValues(48L, 540),
                entityWithPropertyValues(44L, 550)));
        tests.addAll(rangeTest(
                state,
                Values.of(510),
                false,
                Values.of(550),
                false,
                entityWithPropertyValues(43L, 520),
                entityWithPropertyValues(46L, 530),
                entityWithPropertyValues(48L, 540)));
        tests.addAll(rangeTest(
                state,
                null,
                false,
                Values.of(550),
                true,
                entityWithPropertyValues(45L, 500),
                entityWithPropertyValues(42L, 510),
                entityWithPropertyValues(43L, 520),
                entityWithPropertyValues(46L, 530),
                entityWithPropertyValues(48L, 540),
                entityWithPropertyValues(44L, 550)));
        tests.addAll(rangeTest(
                state,
                null,
                true,
                Values.of(550),
                true,
                entityWithPropertyValues(45L, 500),
                entityWithPropertyValues(42L, 510),
                entityWithPropertyValues(43L, 520),
                entityWithPropertyValues(46L, 530),
                entityWithPropertyValues(48L, 540),
                entityWithPropertyValues(44L, 550)));
        tests.addAll(rangeTest(
                state,
                null,
                false,
                Values.of(550),
                false,
                entityWithPropertyValues(45L, 500),
                entityWithPropertyValues(42L, 510),
                entityWithPropertyValues(43L, 520),
                entityWithPropertyValues(46L, 530),
                entityWithPropertyValues(48L, 540)));
        tests.addAll(rangeTest(
                state,
                null,
                true,
                Values.of(550),
                false,
                entityWithPropertyValues(45L, 500),
                entityWithPropertyValues(42L, 510),
                entityWithPropertyValues(43L, 520),
                entityWithPropertyValues(46L, 530),
                entityWithPropertyValues(48L, 540)));
        tests.addAll(rangeTest(
                state,
                Values.of(540),
                true,
                null,
                true,
                entityWithPropertyValues(48L, 540),
                entityWithPropertyValues(44L, 550),
                entityWithPropertyValues(47L, 560)));
        tests.addAll(rangeTest(
                state,
                Values.of(540),
                true,
                null,
                false,
                entityWithPropertyValues(48L, 540),
                entityWithPropertyValues(44L, 550),
                entityWithPropertyValues(47L, 560)));
        tests.addAll(rangeTest(
                state,
                Values.of(540),
                false,
                null,
                true,
                entityWithPropertyValues(44L, 550),
                entityWithPropertyValues(47L, 560)));
        tests.addAll(rangeTest(
                state,
                Values.of(540),
                false,
                null,
                false,
                entityWithPropertyValues(44L, 550),
                entityWithPropertyValues(47L, 560)));
        tests.addAll(rangeTest(state, Values.of(560), false, Values.of(800), true));

        return tests;
    }

    private Collection<DynamicTest> rangeTest(
            ReadableTransactionState state,
            Value lo,
            boolean includeLo,
            Value hi,
            boolean includeHi,
            EntityWithPropertyValues... expected) {
        return Arrays.asList(
                rangeTest(state, IndexOrder.NONE, lo, includeLo, hi, includeHi, expected),
                rangeTest(state, IndexOrder.ASCENDING, lo, includeLo, hi, includeHi, expected),
                rangeTest(state, IndexOrder.DESCENDING, lo, includeLo, hi, includeHi, expected));
    }

    private DynamicTest rangeTest(
            ReadableTransactionState state,
            IndexOrder indexOrder,
            Value lo,
            boolean includeLo,
            Value hi,
            boolean includeHi,
            EntityWithPropertyValues... expected) {
        return DynamicTest.dynamicTest(
                String.format(
                        "range seek: lo=%s (incl: %s), hi=%s (incl: %s), order=%s",
                        lo, includeLo, hi, includeHi, indexOrder),
                () -> {
                    assert lo != NO_VALUE;
                    assert hi != NO_VALUE;
                    final var query = PropertyIndexQuery.range(-1, lo, includeLo, hi, includeHi);
                    final var changes = indexUpdatesForRangeSeek(state, index, new Value[0], query, indexOrder);
                    final var changesWithValues =
                            indexUpdatesWithValuesForRangeSeek(state, index, new Value[0], query, indexOrder);

                    assertContains(indexOrder, changes, changesWithValues, expected);
                });
    }

    @TestFactory
    Collection<DynamicTest> boundingBoxTests() {
        final var state = new TxStateBuilder()
                .withAdded(42L, minPointValueOf(CARTESIAN))
                .withAdded(43L, maxPointValueOf(CARTESIAN))
                .withAdded(44L, pointValue(CARTESIAN, -500, -500))
                .withAdded(45L, pointValue(CARTESIAN, -250, 0))
                .withAdded(46L, pointValue(CARTESIAN, -250, 500))
                .withAdded(47L, pointValue(CARTESIAN, 0, -500))
                .withAdded(48L, pointValue(CARTESIAN, 0, 0))
                .withAdded(49L, pointValue(CARTESIAN, 250, 500))
                .withAdded(50L, pointValue(CARTESIAN, 250, 0))
                .withAdded(51L, pointValue(CARTESIAN, 500, -500))
                .withAdded(52L, pointValue(CARTESIAN, 500, 500))
                .withAdded(53L, minPointValueOf(CARTESIAN_3D))
                .withAdded(54L, maxPointValueOf(CARTESIAN_3D))
                .withAdded(55L, pointValue(CARTESIAN_3D, -500, -500, -500))
                .withAdded(56L, pointValue(CARTESIAN_3D, -500, 0, 250))
                .withAdded(57L, pointValue(CARTESIAN_3D, 0, -250, -250))
                .withAdded(58L, pointValue(CARTESIAN_3D, 0, 0, 0))
                .withAdded(59L, pointValue(CARTESIAN_3D, 0, 250, -500))
                .withAdded(60L, pointValue(CARTESIAN_3D, 250, -250, 500))
                .withAdded(61L, pointValue(CARTESIAN_3D, 500, 500, 500))
                .withAdded(62L, minPointValueOf(WGS_84))
                .withAdded(63L, maxPointValueOf(WGS_84))
                .withAdded(64L, pointValue(WGS_84, -122.322312, 37.563437))
                .withAdded(65L, pointValue(WGS_84, 12.994807, 55.612088))
                .withAdded(66L, pointValue(WGS_84, -0.101008, 51.503773))
                .withAdded(67L, pointValue(WGS_84, 11.572188, 48.135813))
                .withAdded(68L, minPointValueOf(WGS_84_3D))
                .withAdded(69L, maxPointValueOf(WGS_84_3D))
                .withAdded(70L, pointValue(WGS_84_3D, -122.322312, 37.563437, 10))
                .withAdded(71L, pointValue(WGS_84_3D, 12.994807, 55.612088, 0))
                .withAdded(72L, pointValue(WGS_84_3D, -0.101008, 51.503773, 10))
                .withAdded(73L, pointValue(WGS_84_3D, 11.572188, 48.135813, 528))
                .build();

        final var tests = new ArrayList<DynamicTest>();

        tests.add(boundingBoxTest(
                state,
                minPointValueOf(CARTESIAN),
                maxPointValueOf(CARTESIAN),
                entityWithPropertyValues(42L, minPointValueOf(CARTESIAN)),
                entityWithPropertyValues(43L, maxPointValueOf(CARTESIAN)),
                entityWithPropertyValues(44L, pointValue(CARTESIAN, -500, -500)),
                entityWithPropertyValues(45L, pointValue(CARTESIAN, -250, 0)),
                entityWithPropertyValues(46L, pointValue(CARTESIAN, -250, 500)),
                entityWithPropertyValues(47L, pointValue(CARTESIAN, 0, -500)),
                entityWithPropertyValues(48L, pointValue(CARTESIAN, 0, 0)),
                entityWithPropertyValues(49L, pointValue(CARTESIAN, 250, 500)),
                entityWithPropertyValues(50L, pointValue(CARTESIAN, 250, 0)),
                entityWithPropertyValues(51L, pointValue(CARTESIAN, 500, -500)),
                entityWithPropertyValues(52L, pointValue(CARTESIAN, 500, 500))));

        tests.add(boundingBoxTest(
                state,
                pointValue(CARTESIAN, -250, -250),
                pointValue(CARTESIAN, 250, 750),
                entityWithPropertyValues(45L, pointValue(CARTESIAN, -250, 0)),
                entityWithPropertyValues(46L, pointValue(CARTESIAN, -250, 500)),
                entityWithPropertyValues(48L, pointValue(CARTESIAN, 0, 0)),
                entityWithPropertyValues(49L, pointValue(CARTESIAN, 250, 500)),
                entityWithPropertyValues(50L, pointValue(CARTESIAN, 250, 0))));

        tests.add(boundingBoxTest(
                state,
                pointValue(CARTESIAN, -500, -500),
                pointValue(CARTESIAN, 500, 500),
                entityWithPropertyValues(44L, pointValue(CARTESIAN, -500, -500)),
                entityWithPropertyValues(45L, pointValue(CARTESIAN, -250, 0)),
                entityWithPropertyValues(46L, pointValue(CARTESIAN, -250, 500)),
                entityWithPropertyValues(47L, pointValue(CARTESIAN, 0, -500)),
                entityWithPropertyValues(48L, pointValue(CARTESIAN, 0, 0)),
                entityWithPropertyValues(49L, pointValue(CARTESIAN, 250, 500)),
                entityWithPropertyValues(50L, pointValue(CARTESIAN, 250, 0)),
                entityWithPropertyValues(51L, pointValue(CARTESIAN, 500, -500)),
                entityWithPropertyValues(52L, pointValue(CARTESIAN, 500, 500))));

        tests.add(boundingBoxTest(
                state,
                minPointValueOf(CARTESIAN_3D),
                maxPointValueOf(CARTESIAN_3D),
                entityWithPropertyValues(53L, minPointValueOf(CARTESIAN_3D)),
                entityWithPropertyValues(54L, maxPointValueOf(CARTESIAN_3D)),
                entityWithPropertyValues(55L, pointValue(CARTESIAN_3D, -500, -500, -500)),
                entityWithPropertyValues(56L, pointValue(CARTESIAN_3D, -500, 0, 250)),
                entityWithPropertyValues(57L, pointValue(CARTESIAN_3D, 0, -250, -250)),
                entityWithPropertyValues(58L, pointValue(CARTESIAN_3D, 0, 0, 0)),
                entityWithPropertyValues(59L, pointValue(CARTESIAN_3D, 0, 250, -500)),
                entityWithPropertyValues(60L, pointValue(CARTESIAN_3D, 250, -250, 500)),
                entityWithPropertyValues(61L, pointValue(CARTESIAN_3D, 500, 500, 500))));

        tests.add(boundingBoxTest(
                state,
                pointValue(CARTESIAN_3D, -500, -500, -500),
                pointValue(CARTESIAN_3D, 500, 500, 500),
                entityWithPropertyValues(55L, pointValue(CARTESIAN_3D, -500, -500, -500)),
                entityWithPropertyValues(56L, pointValue(CARTESIAN_3D, -500, 0, 250)),
                entityWithPropertyValues(57L, pointValue(CARTESIAN_3D, 0, -250, -250)),
                entityWithPropertyValues(58L, pointValue(CARTESIAN_3D, 0, 0, 0)),
                entityWithPropertyValues(59L, pointValue(CARTESIAN_3D, 0, 250, -500)),
                entityWithPropertyValues(60L, pointValue(CARTESIAN_3D, 250, -250, 500)),
                entityWithPropertyValues(61L, pointValue(CARTESIAN_3D, 500, 500, 500))));

        tests.add(boundingBoxTest(
                state,
                pointValue(CARTESIAN_3D, 0, -250, -500),
                pointValue(CARTESIAN_3D, 250, 250, 500),
                entityWithPropertyValues(57L, pointValue(CARTESIAN_3D, 0, -250, -250)),
                entityWithPropertyValues(58L, pointValue(CARTESIAN_3D, 0, 0, 0)),
                entityWithPropertyValues(59L, pointValue(CARTESIAN_3D, 0, 250, -500)),
                entityWithPropertyValues(60L, pointValue(CARTESIAN_3D, 250, -250, 500))));

        tests.add(boundingBoxTest(
                state,
                minPointValueOf(WGS_84),
                maxPointValueOf(WGS_84),
                entityWithPropertyValues(62L, minPointValueOf(WGS_84)),
                entityWithPropertyValues(63L, maxPointValueOf(WGS_84)),
                entityWithPropertyValues(64L, pointValue(WGS_84, -122.322312, 37.563437)),
                entityWithPropertyValues(65L, pointValue(WGS_84, 12.994807, 55.612088)),
                entityWithPropertyValues(66L, pointValue(WGS_84, -0.101008, 51.503773)),
                entityWithPropertyValues(67L, pointValue(WGS_84, 11.572188, 48.135813))));

        tests.add(boundingBoxTest(
                state,
                pointValue(WGS_84, -171.791110603, 18.91619),
                pointValue(WGS_84, -66.96466, 71.3577635769),
                entityWithPropertyValues(64L, pointValue(WGS_84, -122.322312, 37.563437))));

        tests.add(boundingBoxTest(
                state,
                pointValue(WGS_84, 11.0273686052, 55.3617373725),
                pointValue(WGS_84, 23.9033785336, 69.1062472602),
                entityWithPropertyValues(65L, pointValue(WGS_84, 12.994807, 55.612088))));

        tests.add(boundingBoxTest(
                state,
                pointValue(WGS_84, -7.57216793459, 49.959999905),
                pointValue(WGS_84, 1.68153079591, 58.6350001085),
                entityWithPropertyValues(66L, pointValue(WGS_84, -0.101008, 51.503773))));

        tests.add(boundingBoxTest(
                state,
                pointValue(WGS_84, 5.98865807458, 47.3024876979),
                pointValue(WGS_84, 15.0169958839, 54.983104153),
                entityWithPropertyValues(67L, pointValue(WGS_84, 11.572188, 48.135813))));

        tests.add(boundingBoxTest(
                state,
                pointValue(WGS_84, -0.6, 51.23),
                pointValue(WGS_84, 13.1, 55.65),
                entityWithPropertyValues(65L, pointValue(WGS_84, 12.994807, 55.612088)),
                entityWithPropertyValues(66L, pointValue(WGS_84, -0.101008, 51.503773))));

        tests.add(boundingBoxTest(
                state,
                minPointValueOf(WGS_84_3D),
                maxPointValueOf(WGS_84_3D),
                entityWithPropertyValues(68L, minPointValueOf(WGS_84_3D)),
                entityWithPropertyValues(69L, maxPointValueOf(WGS_84_3D)),
                entityWithPropertyValues(70L, pointValue(WGS_84_3D, -122.322312, 37.563437, 10)),
                entityWithPropertyValues(71L, pointValue(WGS_84_3D, 12.994807, 55.612088, 0)),
                entityWithPropertyValues(72L, pointValue(WGS_84_3D, -0.101008, 51.503773, 10)),
                entityWithPropertyValues(73L, pointValue(WGS_84_3D, 11.572188, 48.135813, 528))));

        tests.add(boundingBoxTest(
                state,
                pointValue(WGS_84_3D, -171.791110603, 18.91619, -86),
                pointValue(WGS_84_3D, -66.96466, 71.3577635769, 6_190.5),
                entityWithPropertyValues(70L, pointValue(WGS_84_3D, -122.322312, 37.563437, 10))));

        tests.add(boundingBoxTest(
                state,
                pointValue(WGS_84_3D, 11.0273686052, 55.3617373725, -2.41),
                pointValue(WGS_84_3D, 23.9033785336, 69.1062472602, 2_097),
                entityWithPropertyValues(71L, pointValue(WGS_84_3D, 12.994807, 55.612088, 0))));

        tests.add(boundingBoxTest(
                state,
                pointValue(WGS_84_3D, -7.57216793459, 49.959999905, -2.75),
                pointValue(WGS_84_3D, 1.68153079591, 58.6350001085, 1_343),
                entityWithPropertyValues(72L, pointValue(WGS_84_3D, -0.101008, 51.503773, 10))));

        tests.add(boundingBoxTest(
                state,
                pointValue(WGS_84_3D, -0.6, 51.23, 0),
                pointValue(WGS_84_3D, 13.1, 55.65, 10),
                entityWithPropertyValues(71L, pointValue(WGS_84_3D, 12.994807, 55.612088, 0)),
                entityWithPropertyValues(72L, pointValue(WGS_84_3D, -0.101008, 51.503773, 10))));

        return tests;
    }

    private DynamicTest boundingBoxTest(
            ReadableTransactionState state,
            PointValue lowerBound,
            PointValue upperBound,
            EntityWithPropertyValues... expected) {
        return DynamicTest.dynamicTest(
                String.format("bounding box seek: lowerBound=%s, upperBound=%s", lowerBound, upperBound), () -> {
                    assert lowerBound != NO_VALUE;
                    assert upperBound != NO_VALUE;
                    final var query = PropertyIndexQuery.boundingBox(-1, lowerBound, upperBound);
                    final var changes = indexUpdatesForBoundingBoxSeek(state, index, new Value[0], query);
                    final var changesWithValues =
                            indexUpdatesWithValuesForBoundingBoxSeek(state, index, new Value[0], query);

                    assertContains(IndexOrder.NONE, changes, changesWithValues, expected);
                });
    }

    @Nested
    class SuffixOrContains {

        @Test
        void shouldComputeIndexUpdatesForRangeSeekByContainsWhenThereAreNoMatchingEntities() {
            // GIVEN
            final ReadableTransactionState state = new TxStateBuilder()
                    .withAdded(42L, "foo")
                    .withAdded(43L, "bar")
                    .build();

            // WHEN
            PropertyIndexQuery.StringContainsPredicate indexQuery =
                    PropertyIndexQuery.stringContains(index.schema().getPropertyId(), stringValue("eulav"));
            AddedAndRemoved changes = indexUpdatesForSuffixOrContains(state, index, indexQuery, IndexOrder.NONE);
            AddedWithValuesAndRemoved changesWithValues =
                    indexUpdatesWithValuesForSuffixOrContains(state, index, indexQuery, IndexOrder.NONE);

            // THEN
            assertTrue(changes.added().isEmpty());
            assertFalse(changesWithValues.added().iterator().hasNext());
        }

        @Test
        void shouldComputeIndexUpdatesForRangeSeekBySuffixWhenThereArePartiallyMatchingNewEntities() {
            // GIVEN
            ReadableTransactionState state = new TxStateBuilder()
                    .withAdded(40L, "Aaron")
                    .withAdded(41L, "Agatha")
                    .withAdded(42L, "Andreas")
                    .withAdded(43L, "Andrea")
                    .withAdded(44L, "Aristotle")
                    .withAdded(45L, "Barbara")
                    .withAdded(46L, "Barbarella")
                    .withAdded(47L, "Cinderella")
                    .build();

            // WHEN
            PropertyIndexQuery.StringSuffixPredicate indexQuery =
                    PropertyIndexQuery.stringSuffix(index.schema().getPropertyId(), stringValue("ella"));
            AddedAndRemoved changes = indexUpdatesForSuffixOrContains(state, index, indexQuery, IndexOrder.NONE);
            AddedWithValuesAndRemoved changesWithValues =
                    indexUpdatesWithValuesForSuffixOrContains(state, index, indexQuery, IndexOrder.NONE);

            // THEN
            assertContains(changes.added(), 46L, 47L);
            assertContains(
                    changesWithValues.added(),
                    entityWithPropertyValues(46L, "Barbarella"),
                    entityWithPropertyValues(47L, "Cinderella"));
        }

        @Test
        void shouldComputeIndexUpdatesForSuffixWithAscendingOrder() {
            assertRangeSeekBySuffixForOrder(IndexOrder.ASCENDING);
        }

        @Test
        void shouldComputeIndexUpdatesForSuffixWithDescendingOrder() {
            assertRangeSeekBySuffixForOrder(IndexOrder.DESCENDING);
        }

        private void assertRangeSeekBySuffixForOrder(IndexOrder indexOrder) {
            // GIVEN
            ReadableTransactionState state = new TxStateBuilder()
                    .withAdded(40L, "Aaron")
                    .withAdded(41L, "Bonbon")
                    .withAdded(42L, "Crayfish")
                    .withAdded(43L, "Mayonnaise")
                    .withAdded(44L, "Seashell")
                    .withAdded(45L, "Ton")
                    .withAdded(46L, "Macron")
                    .withAdded(47L, "Tony")
                    .withAdded(48L, "Evon")
                    .withAdded(49L, "Andromeda")
                    .build();

            // WHEN
            PropertyIndexQuery indexQuery =
                    PropertyIndexQuery.stringSuffix(index.schema().getPropertyId(), stringValue("on"));
            AddedAndRemoved changes = indexUpdatesForSuffixOrContains(state, index, indexQuery, indexOrder);
            AddedWithValuesAndRemoved changesWithValues =
                    indexUpdatesWithValuesForSuffixOrContains(state, index, indexQuery, indexOrder);

            EntityWithPropertyValues[] expected = {
                entityWithPropertyValues(40L, "Aaron"),
                entityWithPropertyValues(41L, "Bonbon"),
                entityWithPropertyValues(48L, "Evon"),
                entityWithPropertyValues(46L, "Macron"),
                entityWithPropertyValues(45L, "Ton")
            };

            // THEN
            assertContains(indexOrder, changes, changesWithValues, expected);
        }

        @Test
        void shouldComputeIndexUpdatesForRangeSeekByContainsWhenThereArePartiallyMatchingNewEntities() {
            // GIVEN
            ReadableTransactionState state = new TxStateBuilder()
                    .withAdded(40L, "Aaron")
                    .withAdded(41L, "Agatha")
                    .withAdded(42L, "Andreas")
                    .withAdded(43L, "Andrea")
                    .withAdded(44L, "Aristotle")
                    .withAdded(45L, "Barbara")
                    .withAdded(46L, "Barbarella")
                    .withAdded(47L, "Cinderella")
                    .build();

            // WHEN
            PropertyIndexQuery.StringContainsPredicate indexQuery =
                    PropertyIndexQuery.stringContains(index.schema().getPropertyId(), stringValue("arbar"));
            AddedAndRemoved changes = indexUpdatesForSuffixOrContains(state, index, indexQuery, IndexOrder.NONE);
            AddedWithValuesAndRemoved changesWithValues =
                    indexUpdatesWithValuesForSuffixOrContains(state, index, indexQuery, IndexOrder.NONE);

            // THEN
            assertContains(changes.added(), 45L, 46L);
            assertContains(
                    changesWithValues.added(),
                    entityWithPropertyValues(45L, "Barbara"),
                    entityWithPropertyValues(46L, "Barbarella"));
        }

        @Test
        void shouldComputeIndexUpdatesForContainsWithAscendingOrder() {
            assertRangeSeekByContainsForOrder(IndexOrder.ASCENDING);
        }

        @Test
        void shouldComputeIndexUpdatesForContainsWithDescendingOrder() {
            assertRangeSeekByContainsForOrder(IndexOrder.DESCENDING);
        }

        private void assertRangeSeekByContainsForOrder(IndexOrder indexOrder) {
            // GIVEN
            ReadableTransactionState state = new TxStateBuilder()
                    .withAdded(40L, "Smashing")
                    .withAdded(41L, "Bashley")
                    .withAdded(42L, "Crasch")
                    .withAdded(43L, "Mayonnaise")
                    .withAdded(44L, "Seashell")
                    .withAdded(45L, "Ton")
                    .withAdded(46L, "The Flash")
                    .withAdded(47L, "Strayhound")
                    .withAdded(48L, "Trashy")
                    .withAdded(49L, "Andromeda")
                    .build();

            // WHEN
            PropertyIndexQuery indexQuery =
                    PropertyIndexQuery.stringContains(index.schema().getPropertyId(), stringValue("ash"));
            AddedAndRemoved changes = indexUpdatesForSuffixOrContains(state, index, indexQuery, indexOrder);
            AddedWithValuesAndRemoved changesWithValues =
                    indexUpdatesWithValuesForSuffixOrContains(state, index, indexQuery, indexOrder);

            EntityWithPropertyValues[] expected = {
                entityWithPropertyValues(41L, "Bashley"),
                entityWithPropertyValues(44L, "Seashell"),
                entityWithPropertyValues(40L, "Smashing"),
                entityWithPropertyValues(46L, "The Flash"),
                entityWithPropertyValues(48L, "Trashy")
            };

            // THEN
            assertContains(indexOrder, changes, changesWithValues, expected);
        }
    }

    @Nested
    class Prefix {

        @Test
        void shouldComputeIndexUpdatesForRangeSeekByPrefixWhenThereAreNoMatchingEntities() {
            // GIVEN
            ReadableTransactionState state = new TxStateBuilder()
                    .withAdded(42L, "value42")
                    .withAdded(43L, "value43")
                    .build();

            // WHEN
            AddedAndRemoved changes =
                    indexUpdatesForRangeSeekByPrefix(state, index, new Value[0], stringValue("eulav"), IndexOrder.NONE);
            AddedWithValuesAndRemoved changesWithValues = indexUpdatesWithValuesForRangeSeekByPrefix(
                    state, index, new Value[0], stringValue("eulav"), IndexOrder.NONE);

            // THEN
            assertTrue(changes.added().isEmpty());
            assertFalse(changesWithValues.added().iterator().hasNext());
        }

        @Test
        void shouldComputeIndexUpdatesForRangeSeekByPrefix() {
            assertRangeSeekByPrefixForOrder(IndexOrder.NONE);
            assertRangeSeekByPrefixForOrder(IndexOrder.ASCENDING);
        }

        @Test
        void shouldComputeIndexUpdatesForRangeSeekByPrefixWithDescendingOrder() {
            assertRangeSeekByPrefixForOrder(IndexOrder.DESCENDING);
        }

        private void assertRangeSeekByPrefixForOrder(IndexOrder indexOrder) {
            // GIVEN
            ReadableTransactionState state = new TxStateBuilder()
                    .withAdded(40L, "Aaron")
                    .withAdded(41L, "Agatha")
                    .withAdded(42L, "Andreas")
                    .withAdded(43L, "Barbarella")
                    .withAdded(44L, "Andrea")
                    .withAdded(45L, "Aristotle")
                    .withAdded(46L, "Barbara")
                    .withAdded(47L, "Andy")
                    .withAdded(48L, "Cinderella")
                    .withAdded(49L, "Andromeda")
                    .build();

            // WHEN
            AddedAndRemoved changes =
                    indexUpdatesForRangeSeekByPrefix(state, index, new Value[0], stringValue("And"), indexOrder);
            AddedWithValuesAndRemoved changesWithValues = indexUpdatesWithValuesForRangeSeekByPrefix(
                    state, index, new Value[0], stringValue("And"), indexOrder);

            EntityWithPropertyValues[] expected = {
                entityWithPropertyValues(44L, "Andrea"),
                entityWithPropertyValues(42L, "Andreas"),
                entityWithPropertyValues(49L, "Andromeda"),
                entityWithPropertyValues(47L, "Andy")
            };

            // THEN
            assertContains(indexOrder, changes, changesWithValues, expected);
        }

        @Test
        void shouldComputeIndexUpdatesForRangeSeekByPrefixWhenThereAreNonStringEntities() {
            // GIVEN
            final ReadableTransactionState state = new TxStateBuilder()
                    .withAdded(42L, "barry")
                    .withAdded(44L, 101L)
                    .withAdded(43L, "bar")
                    .build();

            // WHEN
            AddedAndRemoved changes = TxStateIndexChanges.indexUpdatesForRangeSeekByPrefix(
                    state, index, new Value[0], stringValue("bar"), IndexOrder.NONE);

            // THEN
            assertContainsInOrder(changes.added(), 43L, 42L);
        }
    }

    @Nested
    class CompositeIndex {
        private final IndexDescriptor compositeIndex = TestIndexDescriptorFactory.forLabel(1, 1, 2);
        private final IndexDescriptor compositeIndex3properties = TestIndexDescriptorFactory.forLabel(1, 1, 2, 3);

        @Test
        void shouldSeekOnAnEmptyTxState() {
            // GIVEN
            final ReadableTransactionState state = Mockito.mock(ReadableTransactionState.class);

            // WHEN
            AddedAndRemoved changes = indexUpdatesForSeek(state, compositeIndex, ValueTuple.of("43value1", "43value2"));

            // THEN
            assertTrue(changes.isEmpty());
        }

        @Test
        void shouldScanWhenThereAreNewEntities() {
            // GIVEN
            ReadableTransactionState state = new TxStateBuilder()
                    .withAdded(42L, "42value1", "42value2")
                    .withAdded(43L, "43value1", "43value2")
                    .build();

            // WHEN
            AddedAndRemoved changes = indexUpdatesForScan(state, compositeIndex, IndexOrder.NONE);
            AddedWithValuesAndRemoved changesWithValues =
                    indexUpdatesWithValuesForScan(state, compositeIndex, IndexOrder.NONE);

            // THEN
            assertContains(changes.added(), 42L, 43L);
            assertContains(
                    changesWithValues.added(),
                    entityWithPropertyValues(42L, "42value1", "42value2"),
                    entityWithPropertyValues(43L, "43value1", "43value2"));
        }

        @Test
        void shouldSeekWhenThereAreNewStringEntities() {
            // GIVEN
            ReadableTransactionState state = new TxStateBuilder()
                    .withAdded(42L, "42value1", "42value2")
                    .withAdded(43L, "43value1", "43value2")
                    .build();
            PropertyIndexQuery.RangePredicate<?> predicate =
                    PropertyIndexQuery.range(-1, null, false, stringValue("44val"), true);

            // WHEN
            AddedAndRemoved changes = indexUpdatesForSeek(state, compositeIndex, ValueTuple.of("43value1", "43value2"));
            AddedWithValuesAndRemoved changesWithValues = indexUpdatesWithValuesForRangeSeek(
                    state, compositeIndex, new Value[] {stringValue("43value1")}, predicate, IndexOrder.NONE);

            // THEN
            assertContains(changes.added(), 43L);
            assertContains(changesWithValues.added(), entityWithPropertyValues(43L, "43value1", "43value2"));
        }

        @Test
        void shouldSeekWhenThereAreNewNumberEntities() {
            // GIVEN
            ReadableTransactionState state = new TxStateBuilder()
                    .withAdded(42L, 42001.0, 42002.0)
                    .withAdded(43L, 43001.0, 43002.0)
                    .build();
            PropertyIndexQuery.RangePredicate<?> predicate =
                    PropertyIndexQuery.range(-1, doubleValue(43000.0), true, null, false);

            // WHEN
            AddedAndRemoved changes = indexUpdatesForSeek(state, compositeIndex, ValueTuple.of(43001.0, 43002.0));
            AddedWithValuesAndRemoved changesWithValues = indexUpdatesWithValuesForRangeSeek(
                    state, compositeIndex, new Value[] {doubleValue(43001.0)}, predicate, IndexOrder.NONE);

            // THEN
            assertContains(changes.added(), 43L);
            assertContains(changesWithValues.added(), entityWithPropertyValues(43L, 43001.0, 43002.0));
        }

        @Test
        void shouldHandleMixedAddsAndRemovesEntryForScan() {
            // GIVEN
            ReadableTransactionState state = new TxStateBuilder()
                    .withAdded(42L, "42value1", "42value2")
                    .withAdded(43L, "43value1", "43value2")
                    .withRemoved(43L, "43value1", "43value2")
                    .withRemoved(44L, "44value1", "44value2")
                    .build();

            // WHEN
            AddedAndRemoved changes = indexUpdatesForScan(state, compositeIndex, IndexOrder.NONE);
            AddedWithValuesAndRemoved changesWithValues =
                    indexUpdatesWithValuesForScan(state, compositeIndex, IndexOrder.NONE);

            // THEN
            assertContains(changes.added(), 42L);
            assertContains(changesWithValues.added(), entityWithPropertyValues(42L, "42value1", "42value2"));
            assertContains(changes.removed(), 44L);
            assertContains(changesWithValues.removed(), 44L);
        }

        @Test
        void shouldHandleMixedAddsAndRemovesEntryForSeek() {
            // GIVEN
            ReadableTransactionState state = new TxStateBuilder()
                    .withAdded(42L, "42value1", "42value2")
                    .withAdded(43L, "43value1", "43value2")
                    .withRemoved(43L, "43value1", "43value2")
                    .withRemoved(44L, "44value1", "44value2")
                    .build();

            // WHEN
            AddedAndRemoved changes42 =
                    indexUpdatesForSeek(state, compositeIndex, ValueTuple.of("42value1", "42value2"));
            AddedAndRemoved changes43 =
                    indexUpdatesForSeek(state, compositeIndex, ValueTuple.of("43value1", "43value2"));
            AddedAndRemoved changes44 =
                    indexUpdatesForSeek(state, compositeIndex, ValueTuple.of("44value1", "44value2"));

            AddedWithValuesAndRemoved changesWithValues42 = indexUpdatesWithValuesForRangeSeek(
                    state, compositeIndex, new Value[] {stringValue("42value1")}, null, IndexOrder.NONE);
            AddedWithValuesAndRemoved changesWithValues43 = indexUpdatesWithValuesForRangeSeek(
                    state, compositeIndex, new Value[] {stringValue("43value1")}, null, IndexOrder.NONE);
            AddedWithValuesAndRemoved changesWithValues44 = indexUpdatesWithValuesForRangeSeek(
                    state, compositeIndex, new Value[] {stringValue("44value1")}, null, IndexOrder.NONE);

            // THEN
            assertContains(changes42.added(), 42L);
            assertTrue(changes42.removed().isEmpty());
            assertTrue(changes43.isEmpty());
            assertTrue(changes44.added().isEmpty());
            assertContains(changes44.removed(), 44L);

            assertContains(changesWithValues42.added(), entityWithPropertyValues(42L, "42value1", "42value2"));
            assertTrue(changesWithValues42.removed().isEmpty());
            assertTrue(changesWithValues43.isEmpty());
            assertFalse(changesWithValues44.added().iterator().hasNext());
            assertContains(changesWithValues44.removed(), 44L);
        }

        @Test
        void shouldSeekWhenThereAreManyEntriesWithTheSameValues() {
            // GIVEN (note that 44 has the same properties as 43)
            ReadableTransactionState state = new TxStateBuilder()
                    .withAdded(42L, "42value1", "42value2", "42value3")
                    .withAdded(43L, "43value1", "43value2", "43value3")
                    .withAdded(44L, "43value1", "43value2", "43value3")
                    .withAdded(45L, "43value1", "42value2", "42value3")
                    .build();

            // WHEN
            AddedAndRemoved changes =
                    indexUpdatesForSeek(state, compositeIndex, ValueTuple.of("43value1", "43value2", "43value3"));
            AddedWithValuesAndRemoved changesWithValues = indexUpdatesWithValuesForRangeSeek(
                    state,
                    compositeIndex3properties,
                    new Value[] {stringValue("43value1"), stringValue("43value2")},
                    null,
                    IndexOrder.NONE);

            // THEN
            assertContains(changes.added(), 43L, 44L);
            assertContains(
                    changesWithValues.added(),
                    entityWithPropertyValues(43L, "43value1", "43value2", "43value3"),
                    entityWithPropertyValues(44L, "43value1", "43value2", "43value3"));
        }

        @Test
        void shouldSeekInComplexMix() {
            // GIVEN
            ReadableTransactionState state = new TxStateBuilder()
                    .withAdded(10L, "hi", 3)
                    .withAdded(11L, 9L, 33L)
                    .withAdded(12L, "sneaker", false)
                    .withAdded(13L, new int[] {10, 100}, "array-buddy")
                    .withAdded(14L, 40.1, 40.2)
                    .build();

            // THEN
            assertContains(
                    indexUpdatesForSeek(state, compositeIndex, ValueTuple.of("hi", 3))
                            .added(),
                    10L);
            assertContains(
                    indexUpdatesForSeek(state, compositeIndex, ValueTuple.of(9L, 33L))
                            .added(),
                    11L);
            assertContains(
                    indexUpdatesForSeek(state, compositeIndex, ValueTuple.of("sneaker", false))
                            .added(),
                    12L);
            assertContains(
                    indexUpdatesForSeek(state, compositeIndex, ValueTuple.of(new int[] {10, 100}, "array-buddy"))
                            .added(),
                    13L);
            assertContains(
                    indexUpdatesForSeek(state, compositeIndex, ValueTuple.of(40.1, 40.2))
                            .added(),
                    14L);

            assertContains(
                    indexUpdatesWithValuesForRangeSeek(
                                    state, compositeIndex, new Value[] {stringValue("hi")}, null, IndexOrder.NONE)
                            .added(),
                    entityWithPropertyValues(10L, "hi", 3));
            assertContains(
                    indexUpdatesWithValuesForRangeSeek(
                                    state, compositeIndex, new Value[] {longValue(9L)}, null, IndexOrder.NONE)
                            .added(),
                    entityWithPropertyValues(11L, 9L, 33L));
            assertContains(
                    indexUpdatesWithValuesForRangeSeek(
                                    state, compositeIndex, new Value[] {stringValue("sneaker")}, null, IndexOrder.NONE)
                            .added(),
                    entityWithPropertyValues(12L, "sneaker", false));
            assertContains(
                    indexUpdatesWithValuesForRangeSeek(
                                    state,
                                    compositeIndex,
                                    new Value[] {intArray(new int[] {10, 100})},
                                    null,
                                    IndexOrder.NONE)
                            .added(),
                    entityWithPropertyValues(13L, new int[] {10, 100}, "array-buddy"));
            assertContains(
                    indexUpdatesWithValuesForRangeSeek(
                                    state, compositeIndex, new Value[] {doubleValue(40.1)}, null, IndexOrder.NONE)
                            .added(),
                    entityWithPropertyValues(14L, 40.1, 40.2));
        }

        @Test
        void shouldComputeIndexUpdatesForRangeSeek() {
            assertRangeSeekForOrder(IndexOrder.NONE);
            assertRangeSeekForOrder(IndexOrder.ASCENDING);
            assertRangeSeekForOrder(IndexOrder.DESCENDING);
        }

        @Test
        void shouldComputeIndexUpdatesForRangeSeekByPrefix() {
            assertRangeSeekByPrefixForOrder(IndexOrder.NONE);
            assertRangeSeekByPrefixForOrder(IndexOrder.ASCENDING);
            assertRangeSeekByPrefixForOrder(IndexOrder.DESCENDING);
        }

        private void assertRangeSeekForOrder(IndexOrder indexOrder) {
            final ReadableTransactionState state = new TxStateBuilder()
                    .withAdded(42L, 520, "random42")
                    .withAdded(43L, 510, "random43")
                    .withAdded(44L, 550, "random44")
                    .withAdded(45L, 500, "random45")
                    .withAdded(46L, "530", "random46")
                    .withAdded(47L, "560", "random47")
                    .withAdded(48L, "540", "random48")
                    .build();

            EntityWithPropertyValues[] expectedInt = new EntityWithPropertyValues[] {
                entityWithPropertyValues(43L, 510, "random43"),
                entityWithPropertyValues(42L, 520, "random42"),
                entityWithPropertyValues(44L, 550, "random44")
            };
            EntityWithPropertyValues[] expectedString = new EntityWithPropertyValues[] {
                entityWithPropertyValues(48L, "540", "random48"), entityWithPropertyValues(47L, "560", "random47")
            };

            AddedAndRemoved changesInt = indexUpdatesForRangeSeek(
                    state,
                    compositeIndex,
                    new Value[0],
                    PropertyIndexQuery.range(-1, intValue(500), false, intValue(600), false),
                    indexOrder);
            AddedAndRemoved changesString = indexUpdatesForRangeSeek(
                    state,
                    compositeIndex,
                    new Value[0],
                    PropertyIndexQuery.range(-1, stringValue("530"), false, null, false),
                    indexOrder);
            AddedWithValuesAndRemoved changesWithValuesInt = indexUpdatesWithValuesForRangeSeek(
                    state,
                    compositeIndex,
                    new Value[0],
                    PropertyIndexQuery.range(-1, intValue(500), false, intValue(600), false),
                    indexOrder);
            AddedWithValuesAndRemoved changesWithValuesString = indexUpdatesWithValuesForRangeSeek(
                    state,
                    compositeIndex,
                    new Value[0],
                    PropertyIndexQuery.range(-1, stringValue("530"), false, null, false),
                    indexOrder);

            assertContains(indexOrder, changesInt, changesWithValuesInt, expectedInt);
            assertContains(indexOrder, changesString, changesWithValuesString, expectedString);
        }

        private void assertRangeSeekByPrefixForOrder(IndexOrder indexOrder) {
            // GIVEN
            ReadableTransactionState state = new TxStateBuilder()
                    .withAdded(40L, "Aaron", "Bass")
                    .withAdded(41L, "Agatha", "Christie")
                    .withAdded(42L, "Andreas", "Jona")
                    .withAdded(43L, "Barbarella", "Fonda")
                    .withAdded(44L, "Andrea", "Kormos")
                    .withAdded(45L, "Aristotle", "Nicomachus")
                    .withAdded(46L, "Barbara", "Mikellen")
                    .withAdded(47L, "Andy", "Gallagher")
                    .withAdded(48L, "Cinderella", "Tremaine")
                    .withAdded(49L, "Andromeda", "Black")
                    .build();

            // WHEN
            AddedAndRemoved changes = indexUpdatesForRangeSeekByPrefix(
                    state, compositeIndex, new Value[0], stringValue("And"), indexOrder);
            AddedWithValuesAndRemoved changesWithValues = indexUpdatesWithValuesForRangeSeekByPrefix(
                    state, compositeIndex, new Value[0], stringValue("And"), indexOrder);

            EntityWithPropertyValues[] expected = {
                entityWithPropertyValues(44L, "Andrea", "Kormos"),
                entityWithPropertyValues(42L, "Andreas", "Jona"),
                entityWithPropertyValues(49L, "Andromeda", "Black"),
                entityWithPropertyValues(47L, "Andy", "Gallagher")
            };

            // THEN
            assertContains(indexOrder, changes, changesWithValues, expected);
        }
    }

    private static void assertContains(
            IndexOrder indexOrder,
            AddedAndRemoved changes,
            AddedWithValuesAndRemoved changesWithValues,
            EntityWithPropertyValues[] expected) {
        if (indexOrder == IndexOrder.DESCENDING) {
            ArrayUtils.reverse(expected);
        }

        long[] expectedEntityIds = Arrays.stream(expected)
                .mapToLong(EntityWithPropertyValues::getEntityId)
                .toArray();

        if (indexOrder == IndexOrder.NONE) {
            assertContains(changes.added(), expectedEntityIds);
            assertContains(changesWithValues.added(), expected);
        } else {
            assertContainsInOrder(changes.added(), expectedEntityIds);
            assertContainsInOrder(changesWithValues.added(), expected);
        }
    }

    private static EntityWithPropertyValues entityWithPropertyValues(long entityId, Object... values) {
        return new EntityWithPropertyValues(
                entityId, Arrays.stream(values).map(ValueUtils::of).toArray(Value[]::new));
    }

    private static void assertContains(LongIterable iterable, long... entityIds) {
        assertThat(iterable.toArray()).contains(entityIds);
    }

    private static void assertContains(
            Iterable<EntityWithPropertyValues> iterable, EntityWithPropertyValues... expected) {
        assertThat(iterable).containsExactlyInAnyOrder(expected);
    }

    private static void assertContainsInOrder(LongIterable iterable, long... entityIds) {
        assertThat(iterable.toArray()).containsExactly(entityIds);
    }

    private static void assertContainsInOrder(
            Iterable<EntityWithPropertyValues> iterable, EntityWithPropertyValues... expected) {
        if (expected.length == 0) {
            assertThat(iterable).isEmpty();
        } else {
            assertThat(iterable).containsExactly(expected);
        }
    }

    private static class TxStateBuilder {
        Map<ValueTuple, MutableLongDiffSets> updates = new HashMap<>();

        TxStateBuilder withAdded(long id, Object... value) {
            final ValueTuple valueTuple = ValueTuple.of(value);
            final MutableLongDiffSets changes = updates.computeIfAbsent(
                    valueTuple,
                    ignore -> newMutableLongDiffSets(OnHeapCollectionsFactory.INSTANCE, EmptyMemoryTracker.INSTANCE));
            changes.add(id);
            return this;
        }

        TxStateBuilder withRemoved(long id, Object... value) {
            final ValueTuple valueTuple = ValueTuple.of(value);
            final MutableLongDiffSets changes = updates.computeIfAbsent(
                    valueTuple,
                    ignore -> newMutableLongDiffSets(OnHeapCollectionsFactory.INSTANCE, EmptyMemoryTracker.INSTANCE));
            changes.remove(id);
            return this;
        }

        ReadableTransactionState build() {
            final ReadableTransactionState mock = Mockito.mock(ReadableTransactionState.class);
            doReturn(new UnmodifiableMap<>(updates)).when(mock).getIndexUpdates(any(SchemaDescriptor.class));
            final TreeMap<ValueTuple, MutableLongDiffSets> sortedMap = new TreeMap<>(ValueTuple.COMPARATOR);
            sortedMap.putAll(updates);
            doReturn(sortedMap).when(mock).getSortedIndexUpdates(any(SchemaDescriptor.class));
            return mock;
        }
    }
}
