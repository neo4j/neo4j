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
import org.neo4j.configuration.Config;
import org.neo4j.gis.spatial.index.curves.SpaceFillingCurveConfiguration;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.memory.ByteBufferFactory;
import org.neo4j.kernel.api.index.IndexValueValidator;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettings;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.values.storable.Value;

public class PointBlockBasedIndexPopulator extends BlockBasedIndexPopulator<PointKey> {
    private final IndexSpecificSpaceFillingCurveSettings spatialSettings;
    private final SpaceFillingCurveConfiguration configuration;

    PointBlockBasedIndexPopulator(
            DatabaseIndexContext databaseIndexContext,
            IndexFiles indexFiles,
            IndexLayout<PointKey> layout,
            IndexDescriptor descriptor,
            IndexSpecificSpaceFillingCurveSettings spatialSettings,
            SpaceFillingCurveConfiguration configuration,
            boolean archiveFailedIndex,
            ByteBufferFactory bufferFactory,
            Config config,
            MemoryTracker memoryTracker,
            Monitor monitor,
            ImmutableSet<OpenOption> openOptions) {
        super(
                databaseIndexContext,
                indexFiles,
                layout,
                descriptor,
                archiveFailedIndex,
                bufferFactory,
                config,
                memoryTracker,
                monitor,
                openOptions);
        this.spatialSettings = spatialSettings;
        this.configuration = configuration;
    }

    @Override
    protected IndexValueValidator instantiateValueValidator() {
        // Validation is supposed to check that the to-be-added values fit into the index key.
        // This is always true for the point index, because it does not support composite keys
        // and the supported values have only two possible sizes - serialised 2D point
        // and serialised 3D point.
        // Size of either of them is well under GBP-tree key limit.
        return IndexValueValidator.NO_VALIDATION;
    }

    @Override
    NativeIndexReader<PointKey> newReader() {
        return new PointIndexReader(
                tree, layout, descriptor, spatialSettings, configuration, IndexUsageTracking.NO_USAGE_TRACKING);
    }

    @Override
    protected IndexUpdateIgnoreStrategy indexUpdateIgnoreStrategy() {
        return UPDATE_IGNORE_STRATEGY;
    }

    @Override
    public Map<String, Value> indexConfig() {
        Map<String, Value> map = new HashMap<>();
        spatialSettings.visitIndexSpecificSettings(new SpatialConfigVisitor(map));
        return map;
    }
}
