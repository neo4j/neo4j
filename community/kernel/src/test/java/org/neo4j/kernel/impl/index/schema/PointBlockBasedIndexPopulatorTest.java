/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.index.schema;

import java.io.IOException;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.gis.spatial.index.curves.StandardConfiguration;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.io.memory.ByteBufferFactory;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettings;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public class PointBlockBasedIndexPopulatorTest extends BlockBasedIndexPopulatorTest<PointKey>
{
    private final Config config = Config.defaults( GraphDatabaseInternalSettings.index_populator_merge_factor, 2 );
    private final IndexSpecificSpaceFillingCurveSettings spatialSettings = IndexSpecificSpaceFillingCurveSettings.fromConfig( config );

    @Override
    IndexType indexType()
    {
        return IndexType.POINT;
    }

    @Override
    PointBlockBasedIndexPopulator instantiatePopulator( BlockStorage.Monitor monitor, ByteBufferFactory bufferFactory, MemoryTracker memoryTracker )
            throws IOException
    {
        StandardConfiguration configuration = new StandardConfiguration();
        PointBlockBasedIndexPopulator populator =
                new PointBlockBasedIndexPopulator( databaseIndexContext, indexFiles, layout(), INDEX_DESCRIPTOR, spatialSettings, configuration, false,
                        bufferFactory, config, memoryTracker, monitor );
        populator.create();
        return populator;
    }

    @Override
    PointLayout layout()
    {
        return new PointLayout( spatialSettings );
    }

    @Override
    protected Value supportedValue( int i )
    {
        return Values.pointValue( CoordinateReferenceSystem.Cartesian, i, i );
    }
}
