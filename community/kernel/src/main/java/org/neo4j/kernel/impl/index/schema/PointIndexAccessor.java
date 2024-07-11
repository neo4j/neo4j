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

import static org.neo4j.kernel.impl.index.schema.PointIndexProvider.UPDATE_IGNORE_STRATEGY;

import java.nio.file.OpenOption;
import java.util.HashMap;
import java.util.Map;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.gis.spatial.index.curves.SpaceFillingCurveConfiguration;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettings;
import org.neo4j.values.storable.Value;

class PointIndexAccessor extends NativeIndexAccessor<PointKey> {
    private final IndexSpecificSpaceFillingCurveSettings spaceFillingCurveSettings;
    private final SpaceFillingCurveConfiguration configuration;

    PointIndexAccessor(
            DatabaseIndexContext databaseIndexContext,
            IndexFiles indexFiles,
            IndexLayout<PointKey> layout,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
            IndexDescriptor descriptor,
            IndexSpecificSpaceFillingCurveSettings spaceFillingCurveSettings,
            SpaceFillingCurveConfiguration configuration,
            ImmutableSet<OpenOption> openOptions,
            boolean readOnly) {
        super(databaseIndexContext, indexFiles, layout, descriptor, openOptions, readOnly);
        this.spaceFillingCurveSettings = spaceFillingCurveSettings;
        this.configuration = configuration;
        instantiateTree(recoveryCleanupWorkCollector);
    }

    @Override
    public ValueIndexReader newValueReader(IndexUsageTracking usageTracker) {
        assertOpen();
        return new PointIndexReader(tree, layout, descriptor, spaceFillingCurveSettings, configuration, usageTracker);
    }

    @Override
    public void validateBeforeCommit(long entityId, Value[] tuple) {
        // Validation is supposed to check that the to-be-added values fit into the index key.
        // This is always true for the point index, because it does not support composite keys
        // and the supported values have only two possible sizes - serialised 2D point
        // and serialised 3D point.
        // Size of either of them is well under GBP-tree key limit.
    }

    @Override
    public Map<String, Value> indexConfig() {
        Map<String, Value> map = new HashMap<>();
        spaceFillingCurveSettings.visitIndexSpecificSettings(new SpatialConfigVisitor(map));
        return map;
    }

    @Override
    protected IndexUpdateIgnoreStrategy indexUpdateIgnoreStrategy() {
        return UPDATE_IGNORE_STRATEGY;
    }
}
