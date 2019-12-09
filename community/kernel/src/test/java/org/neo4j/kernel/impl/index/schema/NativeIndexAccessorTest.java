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
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.schema.IndexCapability;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettings;
import org.neo4j.values.storable.ValueType;

import static org.neo4j.internal.schema.IndexPrototype.forSchema;
import static org.neo4j.internal.schema.SchemaDescriptor.forLabel;
import static org.neo4j.kernel.impl.index.schema.ValueCreatorUtil.FRACTION_DUPLICATE_NON_UNIQUE;

class NativeIndexAccessorTest<KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue> extends NativeIndexAccessorTests<KEY, VALUE>
{
    private static final IndexSpecificSpaceFillingCurveSettings spaceFillingCurveSettings =
        IndexSpecificSpaceFillingCurveSettings.fromConfig( Config.defaults() );
    private static final StandardConfiguration configuration = new StandardConfiguration();

    private final AccessorFactory<KEY, VALUE> accessorFactory = (AccessorFactory<KEY, VALUE>) genericAccessorFactory();
    private final ValueType[] supportedTypes = ValueType.values();
    private final IndexLayoutFactory<KEY, VALUE> indexLayoutFactory = (IndexLayoutFactory) () -> new GenericLayout( 1, spaceFillingCurveSettings );
    private final IndexCapability indexCapability = GenericNativeIndexProvider.CAPABILITY;

    @Override
    NativeIndexAccessor<KEY, VALUE> makeAccessor() throws IOException
    {
        return accessorFactory.create( pageCache, fs, indexFiles, layout, RecoveryCleanupWorkCollector.immediate(), monitor, indexDescriptor, false );
    }

    @Override
    IndexCapability indexCapability()
    {
        return indexCapability;
    }

    @Override
    ValueCreatorUtil<KEY, VALUE> createValueCreatorUtil()
    {
        return new ValueCreatorUtil<>( forSchema( forLabel( 42, 666 ) ).withName( "index" ).materialise( 0 ), supportedTypes, FRACTION_DUPLICATE_NON_UNIQUE );
    }

    @Override
    IndexLayout<KEY, VALUE> createLayout()
    {
        return indexLayoutFactory.create();
    }

    /* Helpers */
    private static AccessorFactory<GenericKey, NativeIndexValue> genericAccessorFactory()
    {
        return ( pageCache, fs, storeFiles, layout, cleanup, monitor, descriptor, readOnly ) ->
        {
            DatabaseIndexContext context = DatabaseIndexContext.builder( pageCache, fs ).withMonitor( monitor ).withReadOnly( readOnly ).build();
            return new GenericNativeIndexAccessor( context, storeFiles, layout, cleanup, descriptor, spaceFillingCurveSettings, configuration );
        };
    }

    @FunctionalInterface
    private interface AccessorFactory<KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue>
    {
        NativeIndexAccessor<KEY, VALUE> create( PageCache pageCache, FileSystemAbstraction fs, IndexFiles indexFiles, IndexLayout<KEY, VALUE> layout,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, IndexProvider.Monitor monitor, IndexDescriptor descriptor, boolean readOnly )
                throws IOException;
    }
}
