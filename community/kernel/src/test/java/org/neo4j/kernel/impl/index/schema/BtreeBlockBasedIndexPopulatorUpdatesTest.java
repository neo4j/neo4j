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
import org.neo4j.gis.spatial.index.curves.SpaceFillingCurveConfiguration;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettings;
import org.neo4j.kernel.impl.index.schema.config.SpaceFillingCurveSettingsFactory;

import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.io.memory.ByteBufferFactory.heapBufferFactory;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

class BtreeBlockBasedIndexPopulatorUpdatesTest extends GenericBlockBasedIndexPopulatorUpdatesTest<BtreeKey>
{
    @Override
    IndexType indexType()
    {
        return IndexType.BTREE;
    }

    @Override
    GenericBlockBasedIndexPopulator instantiatePopulator( IndexDescriptor indexDescriptor ) throws IOException
    {
        Config config = Config.defaults();
        IndexSpecificSpaceFillingCurveSettings spatialSettings = IndexSpecificSpaceFillingCurveSettings.fromConfig( config );
        GenericLayout layout = new GenericLayout( 1, spatialSettings );
        SpaceFillingCurveConfiguration configuration = SpaceFillingCurveSettingsFactory.getConfiguredSpaceFillingCurveConfiguration( config );
        GenericBlockBasedIndexPopulator populator =
                new GenericBlockBasedIndexPopulator( databaseIndexContext, indexFiles, layout, indexDescriptor, spatialSettings, configuration, false,
                heapBufferFactory( (int) kibiBytes( 40 ) ), config, INSTANCE, tokenNameLookup );
        populator.create();
        return populator;
    }
}
