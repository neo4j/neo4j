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

import static java.lang.String.format;
import static org.neo4j.kernel.impl.index.schema.NativeIndexKey.Inclusion.HIGH;
import static org.neo4j.kernel.impl.index.schema.NativeIndexKey.Inclusion.LOW;
import static org.neo4j.kernel.impl.index.schema.NativeIndexKey.Inclusion.NEUTRAL;

import java.util.Arrays;
import java.util.List;
import org.neo4j.gis.spatial.index.curves.SpaceFillingCurve;
import org.neo4j.gis.spatial.index.curves.SpaceFillingCurveConfiguration;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.internal.schema.IndexQuery.IndexQueryType;
import org.neo4j.kernel.api.index.BridgingIndexProgressor;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettings;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.ValueGroup;

class PointIndexReader extends NativeIndexReader<PointKey> {
    private final IndexSpecificSpaceFillingCurveSettings spaceFillingCurveSettings;
    private final SpaceFillingCurveConfiguration configuration;

    PointIndexReader(
            GBPTree<PointKey, NullValue> tree,
            IndexLayout<PointKey> layout,
            IndexDescriptor descriptor,
            IndexSpecificSpaceFillingCurveSettings spaceFillingCurveSettings,
            SpaceFillingCurveConfiguration configuration,
            IndexUsageTracking usageTracker) {
        super(tree, layout, descriptor, usageTracker);

        this.spaceFillingCurveSettings = spaceFillingCurveSettings;
        this.configuration = configuration;
    }

    @Override
    void validateQuery(IndexQueryConstraints constraints, PropertyIndexQuery[] predicates) {
        if (predicates.length > 1) {
            throw new IllegalArgumentException(format(
                    "Tried to query a point index with a composite query. "
                            + "Composite queries are not supported by a point index. Query was: %s ",
                    Arrays.toString(predicates)));
        }

        if (constraints.order() != IndexOrder.NONE) {
            throw new IllegalArgumentException(
                    "Tried to query a point index with order. Order is not supported by a point index.");
        }

        validateSupportedPredicates(predicates[0]);
    }

    private void validateSupportedPredicates(PropertyIndexQuery predicate) {
        switch (predicate.type()) {
            case ALL_ENTRIES, EXACT, BOUNDING_BOX:
                return;
            default:
                throw new IllegalArgumentException(format(
                        "Tried to query index with illegal query. Only %s, %s, and %s queries are supported by a point index. Query was: %s",
                        IndexQueryType.ALL_ENTRIES, IndexQueryType.EXACT, IndexQueryType.BOUNDING_BOX, predicate));
        }
    }

    @Override
    public void query(
            IndexProgressor.EntityValueClient client,
            QueryContext context,
            IndexQueryConstraints constraints,
            PropertyIndexQuery... predicates) {
        if (predicates.length == 0) {
            return;
        }

        PropertyIndexQuery predicate = predicates[0];
        if (predicate.type() == IndexQueryType.BOUNDING_BOX) {
            context.monitor().queried(descriptor);
            validateQuery(constraints, predicates);
            PropertyIndexQuery.BoundingBoxPredicate boundingBoxPredicate =
                    (PropertyIndexQuery.BoundingBoxPredicate) predicate;
            try {
                // With GeometryRangeQuery predicate this query changes from a straight-forward: build from/to and
                // seek...
                // into a query that is split into multiple sub-queries.
                BridgingIndexProgressor multiProgressor =
                        new BridgingIndexProgressor(client, descriptor.schema().getPropertyIds());
                client.initializeQuery(descriptor, multiProgressor, false, false, constraints, boundingBoxPredicate);
                double[] from = boundingBoxPredicate.from().coordinate();
                double[] to = boundingBoxPredicate.to().coordinate();
                CoordinateReferenceSystem crs = boundingBoxPredicate.crs();
                SpaceFillingCurve curve = spaceFillingCurveSettings.forCrs(crs);
                List<SpaceFillingCurve.LongRange> ranges = curve.getTilesIntersectingEnvelope(from, to, configuration);
                for (SpaceFillingCurve.LongRange range : ranges) {
                    // Here's a sub-query that we'll have to do for this bounding box.
                    PointKey treeKeyFrom = layout.newKey();
                    PointKey treeKeyTo = layout.newKey();
                    initializeFromToKeys(treeKeyFrom, treeKeyTo);
                    // We can not take query inclusion / exclusion into consideration here because then we risk missing
                    // border values. Always use
                    // Inclusion.LOW / HIGH respectively and filter out points later on.
                    treeKeyFrom.writePointDerived(crs, range.min, LOW);
                    treeKeyTo.writePointDerived(crs, range.max + 1, HIGH);
                    startSeekForInitializedRange(
                            multiProgressor,
                            treeKeyFrom,
                            treeKeyTo,
                            context.cursorContext(),
                            true,
                            constraints,
                            boundingBoxPredicate);
                }
            } catch (IllegalArgumentException e) {
                // Invalid query ranges will cause this state (eg. min>max)
                client.initializeQuery(
                        descriptor, IndexProgressor.EMPTY, false, false, constraints, boundingBoxPredicate);
            }
        } else {
            super.query(client, context, constraints, predicates);
        }
    }

    @Override
    boolean initializeRangeForQuery(PointKey treeKeyFrom, PointKey treeKeyTo, PropertyIndexQuery[] predicates) {
        // if we are here, we made it past the validation, and we know there is only one predicate, and it is either
        // allEntries, or an exact match
        PropertyIndexQuery predicate = predicates[0];
        switch (predicate.type()) {
            case ALL_ENTRIES -> {
                treeKeyFrom.initValueAsLowest(-1, ValueGroup.GEOMETRY);
                treeKeyTo.initValueAsHighest(-1, ValueGroup.GEOMETRY);
            }

            case EXACT -> {
                PropertyIndexQuery.ExactPredicate exactPredicate = (PropertyIndexQuery.ExactPredicate) predicate;
                treeKeyFrom.initFromValue(-1, exactPredicate.value(), NEUTRAL);
                treeKeyTo.initFromValue(-1, exactPredicate.value(), NEUTRAL);
            }

            default -> validateSupportedPredicates(predicate); // throw, just in case
        }

        return false;
    }
}
