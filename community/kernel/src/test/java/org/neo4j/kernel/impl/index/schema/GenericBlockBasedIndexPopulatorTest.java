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
import org.neo4j.internal.schema.IndexType;
import org.neo4j.io.memory.ByteBufferFactory;
import org.neo4j.kernel.api.index.IndexValueValidator;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettings;
import org.neo4j.memory.MemoryTracker;

class GenericBlockBasedIndexPopulatorTest extends BlockBasedIndexPopulatorTest<BtreeKey>
{

    @Override
    IndexType indexType()
    {
        return IndexType.BTREE;
    }

    @Override
    BlockBasedIndexPopulator<BtreeKey> instantiatePopulator( BlockStorage.Monitor monitor, ByteBufferFactory bufferFactory,
            MemoryTracker memoryTracker ) throws IOException
    {
        GenericLayout layout = layout();
        BlockBasedIndexPopulator<BtreeKey> populator =
                new BlockBasedIndexPopulator<>( databaseIndexContext, indexFiles, layout, INDEX_DESCRIPTOR, false, bufferFactory,
                        Config.defaults( GraphDatabaseInternalSettings.index_populator_merge_factor, 2 ),
                        memoryTracker, monitor )
                {
                    @Override
                    NativeIndexReader<BtreeKey> newReader()
                    {
                        throw new UnsupportedOperationException( "Not needed in this test" );
                    }

                    @Override
                    protected IndexValueValidator instantiateValueValidator()
                    {
                        return new GenericIndexKeyValidator( tree.keyValueSizeCap(), descriptor, layout, tokenNameLookup );
                    }
                };
        populator.create();
        return populator;
    }

    @Override
    GenericLayout layout()
    {
        IndexSpecificSpaceFillingCurveSettings spatialSettings = IndexSpecificSpaceFillingCurveSettings.fromConfig( Config.defaults() );
        return new GenericLayout( 1, spatialSettings );
    }

}
