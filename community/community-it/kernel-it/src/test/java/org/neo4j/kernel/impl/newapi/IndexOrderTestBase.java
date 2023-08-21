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
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.constrained;
import static org.neo4j.values.storable.CoordinateReferenceSystem.CARTESIAN;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS_84;
import static org.neo4j.values.storable.Values.pointValue;
import static org.neo4j.values.storable.Values.stringValue;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.configuration.Config;
import org.neo4j.exceptions.KernelException;
import org.neo4j.gis.spatial.index.curves.SpaceFillingCurve;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.ValueIndexCursor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettings;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.OtherThread;
import org.neo4j.test.extension.OtherThreadExtension;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

@ExtendWith(OtherThreadExtension.class)
@ExtendWith(RandomExtension.class)
abstract class IndexOrderTestBase<ENTITY_VALUE_INDEX_CURSOR extends Cursor & ValueIndexCursor>
        extends KernelAPIWriteTestBase<WriteTestSupport> {
    protected static final String DEFAULT_PROPERTY_NAME = "prop";
    protected static final String INDEX_NAME = "myIndex";
    protected static final String COMPOSITE_PROPERTY_1 = "prop1";
    protected static final String COMPOSITE_PROPERTY_2 = "prop2";

    @Inject
    private OtherThread otherThread;

    @Inject
    private RandomSupport random;

    @ParameterizedTest
    @EnumSource(
            value = IndexOrder.class,
            names = {"ASCENDING", "DESCENDING"})
    void shouldRangeSeekInOrderWithTxState(IndexOrder indexOrder) throws Exception {
        List<Pair<Long, Value>> expected = new ArrayList<>();

        try (KernelTransaction tx = beginTransaction()) {
            expected.add(entityWithProp(tx, "hello"));
            entityWithProp(tx, "bellow");
            expected.add(entityWithProp(tx, "schmello"));
            expected.add(entityWithProp(tx, "low"));
            expected.add(entityWithProp(tx, "trello"));
            entityWithProp(tx, "yellow");
            expected.add(entityWithProp(tx, "loww"));
            entityWithProp(tx, "below");
            tx.commit();
        }

        createIndex();

        // when
        try (KernelTransaction tx = beginTransaction()) {
            int prop = tx.tokenRead().propertyKey(DEFAULT_PROPERTY_NAME);
            IndexReadSession index =
                    tx.dataRead().indexReadSession(tx.schemaRead().indexGetForName(INDEX_NAME));

            try (var cursor = getEntityValueIndexCursor(tx)) {
                entityWithProp(tx, "allow");
                expected.add(entityWithProp(tx, "now"));
                expected.add(entityWithProp(tx, "jello"));
                entityWithProp(tx, "willow");

                PropertyIndexQuery query = PropertyIndexQuery.range(prop, "hello", true, "trello", true);

                entityIndexSeek(tx, index, cursor, constrained(indexOrder, true), query);
                assertResultsInOrder(expected, cursor, indexOrder);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = IndexOrder.class,
            names = {"ASCENDING", "DESCENDING"})
    void shouldPrefixSeekInOrder(IndexOrder indexOrder) throws Exception {
        List<Pair<Long, Value>> expected = new ArrayList<>();

        try (KernelTransaction tx = beginTransaction()) {
            expected.add(entityWithProp(tx, "bee hive"));
            entityWithProp(tx, "a");
            expected.add(entityWithProp(tx, "become"));
            expected.add(entityWithProp(tx, "be"));
            expected.add(entityWithProp(tx, "bachelor"));
            entityWithProp(tx, "street smart");
            expected.add(entityWithProp(tx, "builder"));
            entityWithProp(tx, "ceasar");
            tx.commit();
        }

        createIndex();

        // when
        try (KernelTransaction tx = beginTransaction()) {
            int prop = tx.tokenRead().propertyKey(DEFAULT_PROPERTY_NAME);
            IndexReadSession index =
                    tx.dataRead().indexReadSession(tx.schemaRead().indexGetForName(INDEX_NAME));

            try (var cursor = getEntityValueIndexCursor(tx)) {
                entityWithProp(tx, "allow");
                expected.add(entityWithProp(tx, "bastard"));
                expected.add(entityWithProp(tx, "bully"));
                entityWithProp(tx, "willow");

                PropertyIndexQuery query = PropertyIndexQuery.stringPrefix(prop, stringValue("b"));
                entityIndexSeek(tx, index, cursor, constrained(indexOrder, true), query);
                assertResultsInOrder(expected, cursor, indexOrder);
            }
        }
    }

    // this test makes sense primarily for a b-tree index which uses space filling curve,
    // but there is no harm in checking that we get the same result from a range index, too
    @ParameterizedTest
    @EnumSource(
            value = IndexOrder.class,
            names = {"ASCENDING", "DESCENDING"})
    void shouldEntityIndexScanInOrderWithPointsWithinSameTile(IndexOrder indexOrder) throws Exception {
        Config config = Config.defaults();
        IndexSpecificSpaceFillingCurveSettings indexSettings =
                IndexSpecificSpaceFillingCurveSettings.fromConfig(config);
        SpaceFillingCurve curve = indexSettings.forCrs(WGS_84);

        // given
        // Many random points that all are close enough to each other to belong to the same tile on the space filling
        // curve.
        int nbrOfValues = 10000;
        PointValue origin = pointValue(WGS_84, 0.0, 0.0);
        Long derivedValueForCenterPoint = curve.derivedValueFor(origin.coordinate());
        double[] centerPoint = curve.centerPointFor(derivedValueForCenterPoint);
        double xWidthMultiplier = curve.getTileWidth(0, curve.getMaxLevel()) / 2;
        double yWidthMultiplier = curve.getTileWidth(1, curve.getMaxLevel()) / 2;

        List<Pair<Long, Value>> expected = new ArrayList<>();

        try (KernelTransaction tx = beginTransaction()) {
            // NOTE: strings come after points in natural ascending sort order
            expected.add(entityWithProp(tx, "a"));
            expected.add(entityWithProp(tx, "b"));
            for (int i = 0; i < nbrOfValues / 8; i++) {
                double x1 = (random.nextDouble() * 2 - 1) * xWidthMultiplier;
                double x2 = (random.nextDouble() * 2 - 1) * xWidthMultiplier;
                double y1 = (random.nextDouble() * 2 - 1) * yWidthMultiplier;
                double y2 = (random.nextDouble() * 2 - 1) * yWidthMultiplier;
                expected.add(entityWithProp(tx, pointValue(WGS_84, centerPoint[0] + x1, centerPoint[1] + y1)));
                expected.add(entityWithProp(tx, pointValue(WGS_84, centerPoint[0] + x1, centerPoint[1] + y2)));
                expected.add(entityWithProp(tx, pointValue(WGS_84, centerPoint[0] + x2, centerPoint[1] + y1)));
                expected.add(entityWithProp(tx, pointValue(WGS_84, centerPoint[0] + x2, centerPoint[1] + y2)));
            }

            tx.commit();
        }

        createIndex();

        // when
        try (KernelTransaction tx = beginTransaction()) {
            IndexReadSession index =
                    tx.dataRead().indexReadSession(tx.schemaRead().indexGetForName(INDEX_NAME));

            try (var cursor = getEntityValueIndexCursor(tx)) {
                for (int i = 0; i < nbrOfValues / 8; i++) {
                    double x1 = (random.nextDouble() * 2 - 1) * xWidthMultiplier;
                    double x2 = (random.nextDouble() * 2 - 1) * xWidthMultiplier;
                    double y1 = (random.nextDouble() * 2 - 1) * yWidthMultiplier;
                    double y2 = (random.nextDouble() * 2 - 1) * yWidthMultiplier;
                    expected.add(entityWithProp(tx, pointValue(WGS_84, centerPoint[0] + x1, centerPoint[1] + y1)));
                    expected.add(entityWithProp(tx, pointValue(WGS_84, centerPoint[0] + x1, centerPoint[1] + y2)));
                    expected.add(entityWithProp(tx, pointValue(WGS_84, centerPoint[0] + x2, centerPoint[1] + y1)));
                    expected.add(entityWithProp(tx, pointValue(WGS_84, centerPoint[0] + x2, centerPoint[1] + y2)));
                }
                expected.add(entityWithProp(tx, "c"));
                expected.add(entityWithProp(tx, "d"));

                entityIndexScan(tx, index, cursor, constrained(indexOrder, true));
                assertResultsInOrder(expected, cursor, indexOrder);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = IndexOrder.class,
            names = {"ASCENDING", "DESCENDING"})
    void shouldEntityIndexScanInOrderPointsOnly(IndexOrder indexOrder) throws Exception {
        List<Pair<Long, Value>> expected = new ArrayList<>();

        try (KernelTransaction tx = beginTransaction()) {
            Iterator<PointValue> points = generateBox(500_000).iterator();
            while (points.hasNext()) {
                expected.add(entityWithProp(tx, points.next()));
            }

            tx.commit();
        }

        createIndex();

        // when
        try (KernelTransaction tx = beginTransaction()) {
            IndexReadSession index =
                    tx.dataRead().indexReadSession(tx.schemaRead().indexGetForName(INDEX_NAME));

            try (var cursor = getEntityValueIndexCursor(tx)) {
                Iterator<PointValue> points = generateBox(400_000).iterator();
                while (points.hasNext()) {
                    expected.add(entityWithProp(tx, points.next()));
                }

                entityIndexScan(tx, index, cursor, constrained(indexOrder, true));
                assertResultsInOrder(expected, cursor, indexOrder);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = IndexOrder.class,
            names = {"ASCENDING", "DESCENDING"})
    void shouldEntityIndexScanInOrderPointArraysOnly(IndexOrder indexOrder) throws Exception {
        List<Pair<Long, Value>> expected = new ArrayList<>();

        try (KernelTransaction tx = beginTransaction()) {
            Iterator<PointValue[]> arraysOfPoints =
                    generateArraysOfPoints(4, 10_000).iterator();
            while (arraysOfPoints.hasNext()) {
                expected.add(entityWithProp(tx, arraysOfPoints.next()));
            }

            tx.commit();
        }

        createIndex();

        // when
        try (KernelTransaction tx = beginTransaction()) {
            IndexReadSession index =
                    tx.dataRead().indexReadSession(tx.schemaRead().indexGetForName(INDEX_NAME));

            try (var cursor = getEntityValueIndexCursor(tx)) {
                Iterator<PointValue[]> arraysOfPoints =
                        generateArraysOfPoints(4, 100_000).iterator();
                while (arraysOfPoints.hasNext()) {
                    expected.add(entityWithProp(tx, arraysOfPoints.next()));
                }

                entityIndexScan(tx, index, cursor, constrained(indexOrder, true));
                assertResultsInOrder(expected, cursor, indexOrder);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = IndexOrder.class,
            names = {"ASCENDING", "DESCENDING"})
    void shouldEntityIndexScanInOrderWithPointsAndEntitiesBefore(IndexOrder indexOrder) throws Exception {
        List<Pair<Long, Value>> expected = new ArrayList<>();

        try (KernelTransaction tx = beginTransaction()) {
            // NOTE: arrays come before points in natural ascending sort order
            expected.add(entityWithProp(tx, new String[] {"a"}));
            expected.add(entityWithProp(tx, new String[] {"b"}));
            expected.add(entityWithProp(tx, new String[] {"c"}));

            Iterator<PointValue> points = generateBox(500_000).iterator();
            while (points.hasNext()) {
                expected.add(entityWithProp(tx, points.next()));
            }

            tx.commit();
        }

        createIndex();

        // when
        try (KernelTransaction tx = beginTransaction()) {
            IndexReadSession index =
                    tx.dataRead().indexReadSession(tx.schemaRead().indexGetForName(INDEX_NAME));

            try (var cursor = getEntityValueIndexCursor(tx)) {
                expected.add(entityWithProp(tx, new String[] {"d"}));

                Iterator<PointValue> points = generateBox(400_000).iterator();
                while (points.hasNext()) {
                    expected.add(entityWithProp(tx, points.next()));
                }

                entityIndexScan(tx, index, cursor, constrained(indexOrder, true));
                assertResultsInOrder(expected, cursor, indexOrder);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = IndexOrder.class,
            names = {"ASCENDING", "DESCENDING"})
    void shouldEntityIndexScanInOrderWithPointArraysAndEntitiesBefore(IndexOrder indexOrder) throws Exception {
        List<Pair<Long, Value>> expected = new ArrayList<>();

        try (KernelTransaction tx = beginTransaction()) {
            // NOTE: geometric arrays are ordered first; thus using minimum PointValues
            expected.add(entityWithProp(tx, new PointValue[] {PointValue.MIN_VALUE}));
            expected.add(entityWithProp(tx, new PointValue[] {PointValue.MIN_VALUE, PointValue.MIN_VALUE}));
            expected.add(entityWithProp(
                    tx, new PointValue[] {PointValue.MIN_VALUE, PointValue.MIN_VALUE, PointValue.MIN_VALUE}));

            Iterator<PointValue[]> arraysOfPoints =
                    generateArraysOfPoints(4, 10_000).iterator();
            while (arraysOfPoints.hasNext()) {
                expected.add(entityWithProp(tx, arraysOfPoints.next()));
            }

            tx.commit();
        }

        createIndex();

        // when
        try (KernelTransaction tx = beginTransaction()) {
            IndexReadSession index =
                    tx.dataRead().indexReadSession(tx.schemaRead().indexGetForName(INDEX_NAME));

            try (var cursor = getEntityValueIndexCursor(tx)) {
                expected.add(entityWithProp(tx, new PointValue[] {
                    PointValue.MIN_VALUE, PointValue.MIN_VALUE, PointValue.MIN_VALUE, PointValue.MIN_VALUE
                }));

                Iterator<PointValue[]> arraysOfPoints =
                        generateArraysOfPoints(4, 100_000).iterator();
                while (arraysOfPoints.hasNext()) {
                    expected.add(entityWithProp(tx, arraysOfPoints.next()));
                }

                entityIndexScan(tx, index, cursor, constrained(indexOrder, true));
                assertResultsInOrder(expected, cursor, indexOrder);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = IndexOrder.class,
            names = {"ASCENDING", "DESCENDING"})
    void shouldEntityIndexScanInOrderWithPointsAndEntitiesAfter(IndexOrder indexOrder) throws Exception {
        List<Pair<Long, Value>> expected = new ArrayList<>();

        try (KernelTransaction tx = beginTransaction()) {
            Iterator<PointValue> points = generateBox(500_000).iterator();
            while (points.hasNext()) {
                expected.add(entityWithProp(tx, points.next()));
            }

            // NOTE: strings come after points in natural ascending sort order
            expected.add(entityWithProp(tx, "a"));

            tx.commit();
        }

        createIndex();

        // when
        try (KernelTransaction tx = beginTransaction()) {
            IndexReadSession index =
                    tx.dataRead().indexReadSession(tx.schemaRead().indexGetForName(INDEX_NAME));

            try (var cursor = getEntityValueIndexCursor(tx)) {
                Iterator<PointValue> points = generateBox(400_000).iterator();
                while (points.hasNext()) {
                    expected.add(entityWithProp(tx, points.next()));
                }

                expected.add(entityWithProp(tx, "b"));

                entityIndexScan(tx, index, cursor, constrained(indexOrder, true));
                assertResultsInOrder(expected, cursor, indexOrder);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = IndexOrder.class,
            names = {"ASCENDING", "DESCENDING"})
    void shouldEntityIndexScanInOrderWithPointArraysAndEntitiesAfter(IndexOrder indexOrder) throws Exception {
        List<Pair<Long, Value>> expected = new ArrayList<>();

        try (KernelTransaction tx = beginTransaction()) {
            Iterator<PointValue[]> arraysOfPoints =
                    generateArraysOfPoints(4, 10_000).iterator();
            while (arraysOfPoints.hasNext()) {
                expected.add(entityWithProp(tx, arraysOfPoints.next()));
            }

            // NOTE: strings come after geometric arrays in natural ascending sort order
            expected.add(entityWithProp(tx, "a"));

            tx.commit();
        }

        createIndex();

        // when
        try (KernelTransaction tx = beginTransaction()) {
            IndexReadSession index =
                    tx.dataRead().indexReadSession(tx.schemaRead().indexGetForName(INDEX_NAME));

            try (var cursor = getEntityValueIndexCursor(tx)) {
                Iterator<PointValue[]> arraysOfPoints =
                        generateArraysOfPoints(4, 100_000).iterator();
                while (arraysOfPoints.hasNext()) {
                    expected.add(entityWithProp(tx, arraysOfPoints.next()));
                }

                expected.add(entityWithProp(tx, "b"));

                entityIndexScan(tx, index, cursor, constrained(indexOrder, true));
                assertResultsInOrder(expected, cursor, indexOrder);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = IndexOrder.class,
            names = {"ASCENDING", "DESCENDING"})
    void shouldEntityIndexScanInOrderWithPointsAndEntitiesOnBothSides(IndexOrder indexOrder) throws Exception {
        List<Pair<Long, Value>> expected = new ArrayList<>();

        try (KernelTransaction tx = beginTransaction()) {
            // NOTE: arrays come before points in natural ascending sort order
            expected.add(entityWithProp(tx, new String[] {"a"}));
            expected.add(entityWithProp(tx, new String[] {"b"}));
            expected.add(entityWithProp(tx, new String[] {"c"}));

            Iterator<PointValue> points = generateBox(500_000).iterator();
            while (points.hasNext()) {
                expected.add(entityWithProp(tx, points.next()));
            }

            // NOTE: strings come after points in natural ascending sort order
            expected.add(entityWithProp(tx, "a"));
            expected.add(entityWithProp(tx, "b"));
            expected.add(entityWithProp(tx, "c"));

            tx.commit();
        }

        createIndex();

        // when
        try (KernelTransaction tx = beginTransaction()) {
            IndexReadSession index =
                    tx.dataRead().indexReadSession(tx.schemaRead().indexGetForName(INDEX_NAME));

            try (var cursor = getEntityValueIndexCursor(tx)) {
                expected.add(entityWithProp(tx, new String[] {"d"}));

                Iterator<PointValue> points = generateBox(400_000).iterator();
                while (points.hasNext()) {
                    expected.add(entityWithProp(tx, points.next()));
                }

                expected.add(entityWithProp(tx, "d"));

                entityIndexScan(tx, index, cursor, constrained(indexOrder, true));
                assertResultsInOrder(expected, cursor, indexOrder);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = IndexOrder.class,
            names = {"ASCENDING", "DESCENDING"})
    void shouldEntityIndexScanInOrderWithPointArraysAndEntitiesOnBothSides(IndexOrder indexOrder) throws Exception {
        List<Pair<Long, Value>> expected = new ArrayList<>();

        try (KernelTransaction tx = beginTransaction()) {
            // NOTE: geometric arrays are ordered first; thus using minimum PointValues
            expected.add(entityWithProp(tx, new PointValue[] {PointValue.MIN_VALUE}));
            expected.add(entityWithProp(tx, new PointValue[] {PointValue.MIN_VALUE, PointValue.MIN_VALUE}));
            expected.add(entityWithProp(
                    tx, new PointValue[] {PointValue.MIN_VALUE, PointValue.MIN_VALUE, PointValue.MIN_VALUE}));

            Iterator<PointValue[]> arraysOfPoints =
                    generateArraysOfPoints(4, 10_000).iterator();
            while (arraysOfPoints.hasNext()) {
                expected.add(entityWithProp(tx, arraysOfPoints.next()));
            }

            // NOTE: strings come after geometric arrays in natural ascending sort order
            expected.add(entityWithProp(tx, "a"));
            expected.add(entityWithProp(tx, "b"));
            expected.add(entityWithProp(tx, "c"));

            tx.commit();
        }

        createIndex();

        // when
        try (KernelTransaction tx = beginTransaction()) {
            IndexReadSession index =
                    tx.dataRead().indexReadSession(tx.schemaRead().indexGetForName(INDEX_NAME));

            try (var cursor = getEntityValueIndexCursor(tx)) {
                expected.add(entityWithProp(tx, new PointValue[] {
                    PointValue.MIN_VALUE, PointValue.MIN_VALUE, PointValue.MIN_VALUE, PointValue.MIN_VALUE
                }));

                Iterator<PointValue[]> arraysOfPoints =
                        generateArraysOfPoints(4, 100_000).iterator();
                while (arraysOfPoints.hasNext()) {
                    expected.add(entityWithProp(tx, arraysOfPoints.next()));
                }

                expected.add(entityWithProp(tx, "d"));

                entityIndexScan(tx, index, cursor, constrained(indexOrder, true));
                assertResultsInOrder(expected, cursor, indexOrder);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = IndexOrder.class,
            names = {"ASCENDING", "DESCENDING"})
    void shouldEntityIndexScanInOrderPointsAndPointArrays(IndexOrder indexOrder) throws Exception {
        List<Pair<Long, Value>> expected = new ArrayList<>();

        try (KernelTransaction tx = beginTransaction()) {
            // NOTE: geometric arrays are ordered before points
            Iterator<PointValue[]> arraysOfPoints =
                    generateArraysOfPoints(4, 10_000).iterator();
            while (arraysOfPoints.hasNext()) {
                expected.add(entityWithProp(tx, arraysOfPoints.next()));
            }

            Iterator<PointValue> points = generateBox(500_000).iterator();
            while (points.hasNext()) {
                expected.add(entityWithProp(tx, points.next()));
            }

            tx.commit();
        }

        createIndex();

        // when
        try (KernelTransaction tx = beginTransaction()) {
            IndexReadSession index =
                    tx.dataRead().indexReadSession(tx.schemaRead().indexGetForName(INDEX_NAME));

            try (var cursor = getEntityValueIndexCursor(tx)) {
                Iterator<PointValue[]> arraysOfPoints =
                        generateArraysOfPoints(4, 100_000).iterator();
                while (arraysOfPoints.hasNext()) {
                    expected.add(entityWithProp(tx, arraysOfPoints.next()));
                }

                // add points with values in between some points within the geometric array
                Iterator<PointValue> points = generateBox(250_000).iterator();
                while (points.hasNext()) {
                    expected.add(entityWithProp(tx, points.next()));
                }

                entityIndexScan(tx, index, cursor, constrained(indexOrder, true));
                assertResultsInOrder(expected, cursor, indexOrder);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = IndexOrder.class,
            names = {"ASCENDING", "DESCENDING"})
    void shouldEntityIndexScanInOrderPointsAndPointArraysAndEntitiesOnBothSides(IndexOrder indexOrder)
            throws Exception {
        List<Pair<Long, Value>> expected = new ArrayList<>();

        try (KernelTransaction tx = beginTransaction()) {
            // NOTE: geometric arrays are ordered first; thus using minimum PointValues
            expected.add(entityWithProp(tx, new PointValue[] {PointValue.MIN_VALUE}));
            expected.add(entityWithProp(tx, new PointValue[] {PointValue.MIN_VALUE, PointValue.MIN_VALUE}));

            Iterator<PointValue[]> arraysOfPoints =
                    generateArraysOfPoints(4, 10_000).iterator();
            while (arraysOfPoints.hasNext()) {
                expected.add(entityWithProp(tx, arraysOfPoints.next()));
            }

            // NOTE: string arrays are between geometric arrays and points
            expected.add(entityWithProp(tx, new String[] {"a"}));
            expected.add(entityWithProp(tx, new String[] {"b"}));

            Iterator<PointValue> points = generateBox(500_000).iterator();
            while (points.hasNext()) {
                expected.add(entityWithProp(tx, points.next()));
            }

            // NOTE: strings come after points
            expected.add(entityWithProp(tx, "a"));
            expected.add(entityWithProp(tx, "b"));

            tx.commit();
        }

        createIndex();

        // when
        try (KernelTransaction tx = beginTransaction()) {
            IndexReadSession index =
                    tx.dataRead().indexReadSession(tx.schemaRead().indexGetForName(INDEX_NAME));

            try (var cursor = getEntityValueIndexCursor(tx)) {
                expected.add(entityWithProp(
                        tx, new PointValue[] {PointValue.MIN_VALUE, PointValue.MIN_VALUE, PointValue.MIN_VALUE}));

                Iterator<PointValue[]> arraysOfPoints =
                        generateArraysOfPoints(4, 100_000).iterator();
                while (arraysOfPoints.hasNext()) {
                    expected.add(entityWithProp(tx, arraysOfPoints.next()));
                }

                expected.add(entityWithProp(tx, new String[] {"c"}));

                // add points with values in between some points within the geometric array
                Iterator<PointValue> points = generateBox(250_000).iterator();
                while (points.hasNext()) {
                    expected.add(entityWithProp(tx, points.next()));
                }

                expected.add(entityWithProp(tx, "c"));

                entityIndexScan(tx, index, cursor, constrained(indexOrder, true));
                assertResultsInOrder(expected, cursor, indexOrder);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = IndexOrder.class,
            names = {"ASCENDING", "DESCENDING"})
    void shouldDoOrderedCompositeIndexScanWithPointsInBothValues(IndexOrder indexOrder) throws Exception {
        List<Pair<Long, Value[]>> expected = new ArrayList<>();

        try (KernelTransaction tx = beginTransaction()) {
            Iterator<PointValue> points = generateBox(500_000).iterator();
            while (points.hasNext()) {
                PointValue point = points.next();
                expected.add(entityWithTwoProps(tx, point, "a"));
                expected.add(entityWithTwoProps(tx, point, "b"));
                expected.add(entityWithTwoProps(tx, "a", point));
            }

            tx.commit();
        }

        createCompositeIndex();

        // when
        try (KernelTransaction tx = beginTransaction()) {
            IndexReadSession index =
                    tx.dataRead().indexReadSession(tx.schemaRead().indexGetForName(INDEX_NAME));

            try (var cursor = getEntityValueIndexCursor(tx)) {
                entityIndexScan(tx, index, cursor, constrained(indexOrder, true));
                assertCompositeResultsInOrder(expected, cursor, indexOrder);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = IndexOrder.class,
            names = {"ASCENDING", "DESCENDING"})
    void shouldDoOrderedCompositeIndexScanWithPointArraysInBothValues(IndexOrder indexOrder) throws Exception {
        List<Pair<Long, Value[]>> expected = new ArrayList<>();

        try (KernelTransaction tx = beginTransaction()) {
            Iterator<PointValue[]> arraysOfPoints =
                    generateArraysOfPoints(4, 10_000).iterator();
            while (arraysOfPoints.hasNext()) {
                PointValue[] points = arraysOfPoints.next();
                expected.add(entityWithTwoProps(tx, points, "a"));
                expected.add(entityWithTwoProps(tx, points, "b"));
                expected.add(entityWithTwoProps(tx, "a", points));
            }

            tx.commit();
        }

        createCompositeIndex();

        // when
        try (KernelTransaction tx = beginTransaction()) {
            IndexReadSession index =
                    tx.dataRead().indexReadSession(tx.schemaRead().indexGetForName(INDEX_NAME));

            try (var cursor = getEntityValueIndexCursor(tx)) {
                entityIndexScan(tx, index, cursor, constrained(indexOrder, true));
                assertCompositeResultsInOrder(expected, cursor, indexOrder);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = IndexOrder.class,
            names = {"ASCENDING", "DESCENDING"})
    void shouldDoOrderedCompositeIndexScanWithPointsAndPointArraysInBothValues(IndexOrder indexOrder) throws Exception {
        List<Pair<Long, Value[]>> expected = new ArrayList<>();

        try (KernelTransaction tx = beginTransaction()) {
            List<PointValue[]> arraysOfPoints =
                    generateArraysOfPoints(4, 10_000).collect(Collectors.toUnmodifiableList());
            List<PointValue> points = generateBox(500_000).collect(Collectors.toUnmodifiableList());

            int length = Math.min(arraysOfPoints.size(), points.size());
            for (int i = 0; i < length; i++) {
                expected.add(entityWithTwoProps(tx, arraysOfPoints.get(i), points.get(i)));
                expected.add(entityWithTwoProps(tx, points.get(i), arraysOfPoints.get(i)));
            }

            tx.commit();
        }

        createCompositeIndex();

        // when
        try (KernelTransaction tx = beginTransaction()) {
            IndexReadSession index =
                    tx.dataRead().indexReadSession(tx.schemaRead().indexGetForName(INDEX_NAME));

            try (var cursor = getEntityValueIndexCursor(tx)) {
                entityIndexScan(tx, index, cursor, constrained(indexOrder, true));
                assertCompositeResultsInOrder(expected, cursor, indexOrder);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = IndexOrder.class,
            names = {"ASCENDING", "DESCENDING"})
    void shouldDoOrderedCompositeIndexScanWithPointsInBothValuesWithOneGapBetween(IndexOrder indexOrder)
            throws Exception {
        List<Pair<Long, Value[]>> expected = new ArrayList<>();

        try (KernelTransaction tx = beginTransaction()) {
            expected.add(entityWithTwoProps(tx, new String[] {"a"}, new String[] {"b"}));

            Iterator<PointValue> points = generateBox(500_000).iterator();
            while (points.hasNext()) {
                expected.add(entityWithTwoProps(tx, points.next(), "a"));
            }

            expected.add(entityWithTwoProps(tx, "b", new String[] {"b"}));

            points = generateBox(500_000).iterator();
            while (points.hasNext()) {
                expected.add(entityWithTwoProps(tx, "b", points.next()));
            }

            expected.add(entityWithTwoProps(tx, "c", new String[] {"b"}));

            tx.commit();
        }

        createCompositeIndex();

        // when
        try (KernelTransaction tx = beginTransaction()) {
            IndexReadSession index =
                    tx.dataRead().indexReadSession(tx.schemaRead().indexGetForName(INDEX_NAME));

            try (var cursor = getEntityValueIndexCursor(tx)) {
                entityIndexScan(tx, index, cursor, constrained(indexOrder, true));
                assertCompositeResultsInOrder(expected, cursor, indexOrder);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = IndexOrder.class,
            names = {"ASCENDING", "DESCENDING"})
    void shouldDoOrderedCompositeIndexScanWithPointArraysInBothValuesWithOneGapBetween(IndexOrder indexOrder)
            throws Exception {
        List<Pair<Long, Value[]>> expected = new ArrayList<>();

        try (KernelTransaction tx = beginTransaction()) {
            expected.add(entityWithTwoProps(
                    tx, new PointValue[] {PointValue.MIN_VALUE}, new PointValue[] {PointValue.MIN_VALUE}));

            Iterator<PointValue[]> arraysOfPoints =
                    generateArraysOfPoints(4, 10_000).iterator();
            while (arraysOfPoints.hasNext()) {
                expected.add(entityWithTwoProps(tx, arraysOfPoints.next(), "a"));
            }

            expected.add(entityWithTwoProps(tx, "b", new PointValue[] {PointValue.MIN_VALUE}));

            arraysOfPoints = generateArraysOfPoints(4, 10_000).iterator();
            while (arraysOfPoints.hasNext()) {
                expected.add(entityWithTwoProps(tx, "b", arraysOfPoints.next()));
            }

            expected.add(entityWithTwoProps(tx, "c", new PointValue[] {PointValue.MIN_VALUE}));

            tx.commit();
        }

        createCompositeIndex();

        // when
        try (KernelTransaction tx = beginTransaction()) {
            IndexReadSession index =
                    tx.dataRead().indexReadSession(tx.schemaRead().indexGetForName(INDEX_NAME));

            try (var cursor = getEntityValueIndexCursor(tx)) {
                entityIndexScan(tx, index, cursor, constrained(indexOrder, true));
                assertCompositeResultsInOrder(expected, cursor, indexOrder);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = IndexOrder.class,
            names = {"ASCENDING", "DESCENDING"})
    void shouldDoOrderedCompositeIndexScanWithPointsInBothValuesWithTwoGapsBetween(IndexOrder indexOrder)
            throws Exception {
        List<Pair<Long, Value[]>> expected = new ArrayList<>();

        try (KernelTransaction tx = beginTransaction()) {
            expected.add(entityWithTwoProps(tx, new String[] {"a"}, new String[] {"b"}));

            Iterator<PointValue> points = generateBox(500_000).iterator();
            while (points.hasNext()) {
                expected.add(entityWithTwoProps(tx, points.next(), "a"));
            }

            expected.add(entityWithTwoProps(tx, "b", new String[] {"b"}));
            expected.add(entityWithTwoProps(tx, "b", new String[] {"c"}));

            points = generateBox(500_000).iterator();
            while (points.hasNext()) {
                expected.add(entityWithTwoProps(tx, "b", points.next()));
            }

            expected.add(entityWithTwoProps(tx, "c", new String[] {"b"}));

            tx.commit();
        }

        createCompositeIndex();

        // when
        try (KernelTransaction tx = beginTransaction()) {
            IndexReadSession index =
                    tx.dataRead().indexReadSession(tx.schemaRead().indexGetForName(INDEX_NAME));

            try (var cursor = getEntityValueIndexCursor(tx)) {
                entityIndexScan(tx, index, cursor, constrained(indexOrder, true));
                assertCompositeResultsInOrder(expected, cursor, indexOrder);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = IndexOrder.class,
            names = {"ASCENDING", "DESCENDING"})
    void shouldDoOrderedCompositeIndexScanWithPointArraysInBothValuesWithTwoGapsBetween(IndexOrder indexOrder)
            throws Exception {
        List<Pair<Long, Value[]>> expected = new ArrayList<>();

        try (KernelTransaction tx = beginTransaction()) {
            expected.add(entityWithTwoProps(
                    tx, new PointValue[] {PointValue.MIN_VALUE}, new PointValue[] {PointValue.MIN_VALUE}));

            Iterator<PointValue[]> arraysOfPoints =
                    generateArraysOfPoints(4, 10_000).iterator();
            while (arraysOfPoints.hasNext()) {
                expected.add(entityWithTwoProps(tx, arraysOfPoints.next(), "a"));
            }

            expected.add(entityWithTwoProps(tx, "b", new PointValue[] {PointValue.MIN_VALUE}));
            expected.add(entityWithTwoProps(tx, "b", new PointValue[] {PointValue.MIN_VALUE, PointValue.MIN_VALUE}));

            arraysOfPoints = generateArraysOfPoints(4, 10_000).iterator();
            while (arraysOfPoints.hasNext()) {
                expected.add(entityWithTwoProps(tx, "b", arraysOfPoints.next()));
            }

            expected.add(entityWithTwoProps(tx, "c", new PointValue[] {PointValue.MIN_VALUE}));

            tx.commit();
        }

        createCompositeIndex();

        // when
        try (KernelTransaction tx = beginTransaction()) {
            IndexReadSession index =
                    tx.dataRead().indexReadSession(tx.schemaRead().indexGetForName(INDEX_NAME));

            try (var cursor = getEntityValueIndexCursor(tx)) {
                entityIndexScan(tx, index, cursor, constrained(indexOrder, true));
                assertCompositeResultsInOrder(expected, cursor, indexOrder);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = IndexOrder.class,
            names = {"ASCENDING", "DESCENDING"})
    void shouldDoOrderedCompositeIndexScanWithPointsInBothValuesWithGapsAndMiddlePoint(IndexOrder indexOrder)
            throws Exception {
        List<Pair<Long, Value[]>> expected = new ArrayList<>();

        try (KernelTransaction tx = beginTransaction()) {
            expected.add(entityWithTwoProps(tx, new String[] {"a"}, new String[] {"b"}));

            List<PointValue> firstPoints = generateBox(500_000).collect(Collectors.toUnmodifiableList());
            int i = 0;
            for (; i < firstPoints.size() - 1; i++) {
                expected.add(entityWithTwoProps(tx, firstPoints.get(i), "a"));
            }

            expected.add(entityWithTwoProps(tx, "a", firstPoints.get(i)));
            expected.add(entityWithTwoProps(tx, "b", new String[] {"c"}));

            Iterator<PointValue> secondPoints = generateBox(500_000).iterator();
            while (secondPoints.hasNext()) {
                expected.add(entityWithTwoProps(tx, "b", secondPoints.next()));
            }

            expected.add(entityWithTwoProps(tx, "c", new String[] {"b"}));

            tx.commit();
        }

        createCompositeIndex();

        // when
        try (KernelTransaction tx = beginTransaction()) {
            IndexReadSession index =
                    tx.dataRead().indexReadSession(tx.schemaRead().indexGetForName(INDEX_NAME));

            try (var cursor = getEntityValueIndexCursor(tx)) {
                entityIndexScan(tx, index, cursor, constrained(indexOrder, true));
                assertCompositeResultsInOrder(expected, cursor, indexOrder);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = IndexOrder.class,
            names = {"ASCENDING", "DESCENDING"})
    void shouldDoOrderedCompositeIndexScanWithPointArraysInBothValuesWithGapsAndMiddlePointArray(IndexOrder indexOrder)
            throws Exception {
        List<Pair<Long, Value[]>> expected = new ArrayList<>();

        try (KernelTransaction tx = beginTransaction()) {
            expected.add(entityWithTwoProps(
                    tx, new PointValue[] {PointValue.MIN_VALUE}, new PointValue[] {PointValue.MIN_VALUE}));

            List<PointValue[]> firstArraysOfPoints =
                    generateArraysOfPoints(4, 10_000).collect(Collectors.toUnmodifiableList());
            int i = 0;
            for (; i < firstArraysOfPoints.size() - 1; i++) {
                expected.add(entityWithTwoProps(tx, firstArraysOfPoints.get(i), "a"));
            }

            expected.add(entityWithTwoProps(tx, "a", firstArraysOfPoints.get(i)));
            expected.add(entityWithTwoProps(tx, "b", new PointValue[] {PointValue.MIN_VALUE}));

            Iterator<PointValue[]> secondArraysOfPoints =
                    generateArraysOfPoints(4, 10_000).iterator();
            while (secondArraysOfPoints.hasNext()) {
                expected.add(entityWithTwoProps(tx, "b", secondArraysOfPoints.next()));
            }

            expected.add(entityWithTwoProps(tx, "c", new PointValue[] {PointValue.MIN_VALUE}));

            tx.commit();
        }

        createCompositeIndex();

        // when
        try (KernelTransaction tx = beginTransaction()) {
            IndexReadSession index =
                    tx.dataRead().indexReadSession(tx.schemaRead().indexGetForName(INDEX_NAME));

            try (var cursor = getEntityValueIndexCursor(tx)) {
                entityIndexScan(tx, index, cursor, constrained(indexOrder, true));
                assertCompositeResultsInOrder(expected, cursor, indexOrder);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = IndexOrder.class,
            names = {"ASCENDING", "DESCENDING"})
    void shouldDoOrderedCompositeIndexScanWithPointsAndPointArraysAndEntitiesMixed(IndexOrder indexOrder)
            throws Exception {
        List<Pair<Long, Value[]>> expected = new ArrayList<>();

        try (KernelTransaction tx = beginTransaction()) {
            List<PointValue[]> arraysOfPoints =
                    generateArraysOfPoints(4, 10_000).collect(Collectors.toUnmodifiableList());
            List<PointValue> points = generateBox(500_000).collect(Collectors.toUnmodifiableList());

            int length = Math.min(arraysOfPoints.size(), points.size());
            for (int i = 0; i < length; i++) {
                expected.add(entityWithTwoProps(tx, arraysOfPoints.get(i), points.get(i)));
                expected.add(entityWithTwoProps(tx, arraysOfPoints.get(i), "a"));
                expected.add(entityWithTwoProps(tx, points.get(i), arraysOfPoints.get(i)));
                expected.add(entityWithTwoProps(tx, points.get(i), "a"));
                expected.add(entityWithTwoProps(tx, "a", arraysOfPoints.get(i)));
                expected.add(entityWithTwoProps(tx, "a", points.get(i)));
            }

            tx.commit();
        }

        createCompositeIndex();

        // when
        try (KernelTransaction tx = beginTransaction()) {
            IndexReadSession index =
                    tx.dataRead().indexReadSession(tx.schemaRead().indexGetForName(INDEX_NAME));

            try (var cursor = getEntityValueIndexCursor(tx)) {
                entityIndexScan(tx, index, cursor, constrained(indexOrder, true));
                assertCompositeResultsInOrder(expected, cursor, indexOrder);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = IndexOrder.class,
            names = {"ASCENDING", "DESCENDING"})
    void shouldEntityIndexSeekInOrderWithStringInMemoryAndConcurrentUpdate(IndexOrder indexOrder) throws Exception {
        String a = "a";
        String b = "b";
        String c = "c";

        createIndex();

        TextValue expectedFirst = indexOrder == IndexOrder.ASCENDING ? stringValue(a) : stringValue(c);
        TextValue expectedLast = indexOrder == IndexOrder.ASCENDING ? stringValue(c) : stringValue(a);
        try (KernelTransaction tx = beginTransaction()) {
            int prop = tx.tokenRead().propertyKey(DEFAULT_PROPERTY_NAME);
            entityWithProp(tx, a);
            entityWithProp(tx, c);

            IndexReadSession index =
                    tx.dataRead().indexReadSession(tx.schemaRead().indexGetForName(INDEX_NAME));

            try (var cursor = getEntityValueIndexCursor(tx)) {

                PropertyIndexQuery query = PropertyIndexQuery.stringPrefix(prop, stringValue(""));
                entityIndexSeek(tx, index, cursor, constrained(indexOrder, true), query);

                assertTrue(cursor.next());
                assertThat(cursor.propertyValue(0)).isEqualTo(expectedFirst);

                assertTrue(cursor.next());
                assertThat(cursor.propertyValue(0)).isEqualTo(expectedLast);

                concurrentInsert(b);

                assertFalse(
                        cursor.next(),
                        () -> "Did not expect to find anything more but found " + cursor.propertyValue(0));
            }
            tx.commit();
        }

        // Verify we see all data in the end
        try (KernelTransaction tx = beginTransaction()) {
            int prop = tx.tokenRead().propertyKey(DEFAULT_PROPERTY_NAME);
            IndexReadSession index =
                    tx.dataRead().indexReadSession(tx.schemaRead().indexGetForName(INDEX_NAME));
            try (var cursor = getEntityValueIndexCursor(tx)) {
                PropertyIndexQuery query = PropertyIndexQuery.stringPrefix(prop, stringValue(""));
                entityIndexSeek(tx, index, cursor, constrained(indexOrder, true), query);
                assertTrue(cursor.next());
                assertThat(cursor.propertyValue(0)).isEqualTo(expectedFirst);

                assertTrue(cursor.next());
                assertThat(cursor.propertyValue(0)).isEqualTo(stringValue(b));

                assertTrue(cursor.next());
                assertThat(cursor.propertyValue(0)).isEqualTo(expectedLast);

                assertFalse(cursor.next());
            }
        }
    }

    private void concurrentInsert(Object value) throws InterruptedException, java.util.concurrent.ExecutionException {
        otherThread
                .execute(() -> {
                    try (KernelTransaction otherTx = beginTransaction()) {
                        entityWithProp(otherTx, value);
                        otherTx.commit();
                    }
                    return null;
                })
                .get();
    }

    private static Stream<PointValue> generateBox(double scale) {
        return Stream.of(IntStream.of(-1, -1), IntStream.of(+1, -1), IntStream.of(-1, +1), IntStream.of(+1, +1))
                .map(rs -> rs.mapToDouble(r -> r * scale))
                .map(rs -> pointValue(CARTESIAN, rs.toArray()));
    }

    // Values.of cannot take an instance of Value; have to pass as Object[]
    private static Stream<PointValue[]> generateArraysOfPoints(int n, double scale) {
        return IntStream.rangeClosed(1, n)
                .mapToObj(i -> generateBox(i * scale))
                .map(points -> points.toArray(PointValue[]::new));
    }

    protected void assertResultsInOrder(
            List<Pair<Long, Value>> expected, ENTITY_VALUE_INDEX_CURSOR cursor, IndexOrder indexOrder) {
        Comparator<Pair<Long, Value>> comparator = Comparator.comparing(Pair::other, Values.COMPARATOR);
        expected.sort(indexOrder == IndexOrder.ASCENDING ? comparator : comparator.reversed());

        Iterator<Pair<Long, Value>> expectedRows = expected.iterator();
        while (cursor.next() && expectedRows.hasNext()) {
            Pair<Long, Value> expectedRow = expectedRows.next();
            assertThat(entityReference(cursor))
                    .as(expectedRow.other() + " == " + cursor.propertyValue(0))
                    .isEqualTo(expectedRow.first());
            for (int i = 0; i < cursor.numberOfProperties(); i++) {
                Value value = cursor.propertyValue(i);
                assertThat(value).isEqualTo(expectedRow.other());
            }
        }

        assertFalse(expectedRows.hasNext());
        assertFalse(cursor.next());
    }

    private void assertCompositeResultsInOrder(
            List<Pair<Long, Value[]>> expected, ENTITY_VALUE_INDEX_CURSOR cursor, IndexOrder indexOrder) {
        Comparator<Pair<Long, Value[]>> comparator = (a, b) -> {
            int compare = Values.COMPARATOR.compare(a.other()[0], b.other()[0]);
            return compare != 0 ? compare : Values.COMPARATOR.compare(a.other()[1], b.other()[1]);
        };
        expected.sort(indexOrder == IndexOrder.ASCENDING ? comparator : comparator.reversed());

        Iterator<Pair<Long, Value[]>> expectedRows = expected.iterator();
        while (cursor.next() && expectedRows.hasNext()) {
            Pair<Long, Value[]> expectedRow = expectedRows.next();
            assertThat(entityReference(cursor))
                    .as(expectedRow.other()[0] + " == " + cursor.propertyValue(0) + " && "
                            + expectedRow.other()[1] + " == " + cursor.propertyValue(1))
                    .isEqualTo(expectedRow.first());
            for (int i = 0; i < cursor.numberOfProperties(); i++) {
                Value value = cursor.propertyValue(i);
                assertThat(value).isEqualTo(expectedRow.other()[i]);
            }
        }

        assertFalse(expectedRows.hasNext());
        assertFalse(cursor.next());
    }

    @Override
    public WriteTestSupport newTestSupport() {
        return new WriteTestSupport();
    }

    protected abstract void createIndex();

    protected abstract void createCompositeIndex();

    protected abstract long entityReference(ENTITY_VALUE_INDEX_CURSOR cursor);

    protected abstract Pair<Long, Value> entityWithProp(KernelTransaction tx, Object value) throws Exception;

    protected abstract Pair<Long, Value[]> entityWithTwoProps(KernelTransaction tx, Object value1, Object value2)
            throws Exception;

    protected abstract ENTITY_VALUE_INDEX_CURSOR getEntityValueIndexCursor(KernelTransaction tx);

    protected abstract void entityIndexScan(
            KernelTransaction tx,
            IndexReadSession index,
            ENTITY_VALUE_INDEX_CURSOR cursor,
            IndexQueryConstraints constraints)
            throws KernelException;

    protected abstract void entityIndexSeek(
            KernelTransaction tx,
            IndexReadSession index,
            ENTITY_VALUE_INDEX_CURSOR cursor,
            IndexQueryConstraints constraints,
            PropertyIndexQuery query)
            throws KernelException;
}
