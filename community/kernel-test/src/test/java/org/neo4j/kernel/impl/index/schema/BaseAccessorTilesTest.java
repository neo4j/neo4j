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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unorderedValues;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.kernel.impl.index.schema.IndexUsageTracking.NO_USAGE_TRACKING;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS_84;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.configuration.Config;
import org.neo4j.gis.spatial.index.curves.SpaceFillingCurve;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettings;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.schema.SimpleEntityValueClient;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

@PageCacheExtension
@ExtendWith(RandomExtension.class)
abstract class BaseAccessorTilesTest<KEY extends NativeIndexKey<KEY>> {
    private static final CoordinateReferenceSystem crs = CoordinateReferenceSystem.WGS_84;
    private static final Config config = Config.defaults();
    static final IndexSpecificSpaceFillingCurveSettings indexSettings =
            IndexSpecificSpaceFillingCurveSettings.fromConfig(config);
    static final SpaceFillingCurve curve = indexSettings.forCrs(crs);

    @Inject
    FileSystemAbstraction fs;

    @Inject
    TestDirectory directory;

    @Inject
    PageCache pageCache;

    CursorContextFactory contextFactory;

    @Inject
    RandomSupport random;

    private NativeIndexAccessor<KEY> accessor;
    IndexDescriptor descriptor;

    abstract IndexDescriptor createDescriptor();

    abstract NativeIndexAccessor<KEY> createAccessor();

    @BeforeEach
    void setup() {
        contextFactory = new CursorContextFactory(new DefaultPageCacheTracer(), EMPTY_CONTEXT_SUPPLIER);
        descriptor = createDescriptor();
        accessor = createAccessor();
    }

    @AfterEach
    void tearDown() {
        accessor.close();
    }

    /**
     * This test verify that we correctly handle unique points that all belong to the same tile on the space filling curve.
     * All points share at least one dimension coordinate with another point to exercise minimal splitter.
     * We verify this by asserting that we always get exactly one hit on an exact match and that the value is what we expect.
     */
    @Test
    void mustHandlePointsWithinSameTile() throws IndexEntryConflictException, IndexNotApplicableKernelException {
        // given
        // Many random points that all are close enough to each other to belong to the same tile on the space filling
        // curve.
        int nbrOfValues = 10000;
        PointValue origin = Values.pointValue(WGS_84, 0.0, 0.0);
        Long derivedValueForCenterPoint = curve.derivedValueFor(origin.coordinate());
        double[] centerPoint = curve.centerPointFor(derivedValueForCenterPoint);
        double xWidthMultiplier = curve.getTileWidth(0, curve.getMaxLevel()) / 2;
        double yWidthMultiplier = curve.getTileWidth(1, curve.getMaxLevel()) / 2;

        List<Value> pointValues = new ArrayList<>();
        List<IndexEntryUpdate<IndexDescriptor>> updates = new ArrayList<>();
        long nodeId = 1;
        for (int i = 0; i < nbrOfValues / 4; i++) {
            double x1 = (random.nextDouble() * 2 - 1) * xWidthMultiplier;
            double x2 = (random.nextDouble() * 2 - 1) * xWidthMultiplier;
            double y1 = (random.nextDouble() * 2 - 1) * yWidthMultiplier;
            double y2 = (random.nextDouble() * 2 - 1) * yWidthMultiplier;
            PointValue value11 = Values.pointValue(WGS_84, centerPoint[0] + x1, centerPoint[1] + y1);
            PointValue value12 = Values.pointValue(WGS_84, centerPoint[0] + x1, centerPoint[1] + y2);
            PointValue value21 = Values.pointValue(WGS_84, centerPoint[0] + x2, centerPoint[1] + y1);
            PointValue value22 = Values.pointValue(WGS_84, centerPoint[0] + x2, centerPoint[1] + y2);
            assertDerivedValue(derivedValueForCenterPoint, value11, value12, value21, value22);

            nodeId = addPointsToLists(pointValues, updates, nodeId, value11, value12, value21, value22);
        }

        processAll(updates);

        // then
        exactMatchOnAllValues(pointValues);
    }

    /**
     * The test mustHandlePointArraysWithinSameTile was flaky on random numbers that placed points just
     * within the tile upper bound, and allocated points to adjacent tiles due to rounding errors.
     * This test uses a specific point that triggers that exact failure in a non-flaky way.
     */
    @Test
    void shouldNotGetRoundingErrorsWithPointsJustWithinTheTileUpperBound() {
        PointValue origin = Values.pointValue(WGS_84, 0.0, 0.0);
        long derivedValueForCenterPoint = curve.derivedValueFor(origin.coordinate());
        double[] centerPoint =
                curve.centerPointFor(derivedValueForCenterPoint); // [1.6763806343078613E-7, 8.381903171539307E-8]

        double xWidthMultiplier = curve.getTileWidth(0, curve.getMaxLevel()) / 2; // 1.6763806343078613E-7
        double yWidthMultiplier = curve.getTileWidth(1, curve.getMaxLevel()) / 2; // 8.381903171539307E-8

        double[] faultyCoords = {1.874410632171803E-8, 1.6763806281859016E-7};

        assertTrue(centerPoint[0] + xWidthMultiplier > faultyCoords[0], "inside upper x limit");
        assertTrue(centerPoint[0] - xWidthMultiplier < faultyCoords[0], "inside lower x limit");

        assertTrue(centerPoint[1] + yWidthMultiplier > faultyCoords[1], "inside upper y limit");
        assertTrue(centerPoint[1] - yWidthMultiplier < faultyCoords[1], "inside lower y limit");

        long derivedValueForFaultyCoords = curve.derivedValueFor(faultyCoords);
        assertEquals(derivedValueForCenterPoint, derivedValueForFaultyCoords, "expected same derived value");
    }

    /**
     * Given how the spatial index works, if a point is on a tile that intersect with the search area,
     * but the point itself is outside the search area, it is a candidate for a false positive.
     * The reason is that such a point is returned from the index itself,
     * because a space index compares (and generally works with) only values of a space-filling curve
     * and not the points themselves.
     * Therefore {@link IndexReader} implementation must post-process raw index results and filter out such false positives.
     */
    @Test
    void shouldNotGetFalsePositivesForRangesSpanningMultipleTiles()
            throws IndexNotApplicableKernelException, IndexEntryConflictException {
        PointValue origin = Values.pointValue(WGS_84, 0.0, 0.0);
        long derivedValueForCenterPoint = curve.derivedValueFor(origin.coordinate());
        double[] searchStart = curve.centerPointFor(derivedValueForCenterPoint);

        double xTileWidth = curve.getTileWidth(0, curve.getMaxLevel());

        // to make it easier to imagine this, the search start is a center point of one tile and the limit is a center
        // point of the next tile on the x-axis
        PointValue limitPoint = Values.pointValue(WGS_84, searchStart[0] + xTileWidth, searchStart[1]);

        int nbrOfValues = 10_000;

        List<PointValue> pointsInside = new ArrayList<>();
        List<IndexEntryUpdate<IndexDescriptor>> updates = new ArrayList<>();

        for (int i = 0; i < nbrOfValues; i++) {
            double distanceMultiplier = random.nextDouble() * 2;
            PointValue point =
                    Values.pointValue(WGS_84, searchStart[0] + distanceMultiplier * xTileWidth, searchStart[1]);

            updates.add(IndexEntryUpdate.add(0, descriptor, point));

            if (distanceMultiplier <= 1) {
                pointsInside.add(point);
            }
        }

        processAll(updates);

        try (var indexReader = accessor.newValueReader(NO_USAGE_TRACKING)) {
            SimpleEntityValueClient client = new SimpleEntityValueClient();

            var boundingBox = PropertyIndexQuery.boundingBox(
                    descriptor.schema().getPropertyId(), Values.pointValue(WGS_84, searchStart), limitPoint);
            indexReader.query(client, QueryContext.NULL_CONTEXT, unorderedValues(), boundingBox);

            List<Value> queryResult = new ArrayList<>();
            while (client.next()) {
                queryResult.add(client.values[0]);
            }

            assertThat(queryResult).containsExactlyInAnyOrderElementsOf(pointsInside);
        }
    }

    private long addPointsToLists(
            List<Value> pointValues,
            List<IndexEntryUpdate<IndexDescriptor>> updates,
            long nodeId,
            PointValue... values) {
        for (PointValue value : values) {
            pointValues.add(value);
            updates.add(IndexEntryUpdate.add(nodeId++, descriptor, value));
        }
        return nodeId;
    }

    static void assertDerivedValue(Long targetDerivedValue, PointValue... values) {
        for (PointValue value : values) {
            Long derivedValueForValue = curve.derivedValueFor(value.coordinate());
            assertEquals(
                    targetDerivedValue,
                    derivedValueForValue,
                    "expected random value to belong to same tile as center point");
        }
    }

    void processAll(List<IndexEntryUpdate<IndexDescriptor>> updates) throws IndexEntryConflictException {
        try (NativeIndexUpdater<KEY> updater = accessor.newUpdater(IndexUpdateMode.ONLINE, NULL_CONTEXT, false)) {
            for (IndexEntryUpdate<IndexDescriptor> update : updates) {
                updater.process(update);
            }
        }
    }

    void exactMatchOnAllValues(List<Value> values) throws IndexNotApplicableKernelException {
        try (var indexReader = accessor.newValueReader(NO_USAGE_TRACKING)) {
            SimpleEntityValueClient client = new SimpleEntityValueClient();
            for (Value value : values) {
                PropertyIndexQuery.ExactPredicate exact =
                        PropertyIndexQuery.exact(descriptor.schema().getPropertyId(), value);
                indexReader.query(client, QueryContext.NULL_CONTEXT, unorderedValues(), exact);

                // then
                assertTrue(client.next());
                assertEquals(value, client.values[0]);
                assertFalse(client.next());
            }
        }
    }
}
