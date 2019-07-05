/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import org.neo4j.gis.spatial.index.curves.StandardConfiguration;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettings;

import static org.neo4j.kernel.impl.index.schema.ByteBufferFactory.heapBufferFactory;

class NativeIndexPopulatorTestCases
{
    static final IndexSpecificSpaceFillingCurveSettings spaceFillingCurveSettings =
            IndexSpecificSpaceFillingCurveSettings.fromConfig( Config.defaults() );
    private static final StandardConfiguration configuration = new StandardConfiguration();

    static PopulatorFactory<GenericKey,NativeIndexValue> genericPopulatorFactory()
    {
        return ( pageCache, fs, storeFile, layout, monitor, descriptor ) ->
                new GenericNativeIndexPopulator( pageCache, fs, storeFile, layout, monitor, descriptor, spaceFillingCurveSettings, configuration, false );
    }

    static PopulatorFactory<GenericKey,NativeIndexValue> genericBlockBasedPopulatorFactory()
    {
        return ( pageCache, fs, storeFile, layout, monitor, descriptor ) ->
                new GenericBlockBasedIndexPopulator( pageCache, fs, storeFile, layout, monitor, descriptor, spaceFillingCurveSettings, configuration, false,
                        heapBufferFactory( 10 * 1024 ) );
    }

    @FunctionalInterface
    public interface PopulatorFactory<KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue>
    {
        NativeIndexPopulator<KEY,VALUE> create( PageCache pageCache, FileSystemAbstraction fs, IndexFiles indexFiles, IndexLayout<KEY,VALUE> layout,
                IndexProvider.Monitor monitor, IndexDescriptor descriptor ) throws IOException;
    }
}
