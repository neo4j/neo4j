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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.schema.IndexPrototype.forSchema;
import static org.neo4j.internal.schema.IndexPrototype.uniqueForSchema;
import static org.neo4j.internal.schema.SchemaDescriptor.forAnyEntityTokens;
import static org.neo4j.internal.schema.SchemaDescriptor.forLabel;
import static org.neo4j.io.pagecache.context.CursorContext.NULL;
import static org.neo4j.kernel.impl.api.index.TestIndexProviderDescriptor.PROVIDER_DESCRIPTOR;

class NativeIndexProviderTest extends IndexProviderTests
{
    private static final ProviderFactory factory =
            ( pageCache, fs, dir, monitors, collector, readOnlyChecker, databaseLayout ) -> {
                DatabaseIndexContext context = DatabaseIndexContext.builder( pageCache, fs, DEFAULT_DATABASE_NAME ).withMonitors( monitors )
                        .withReadOnlyChecker( readOnlyChecker ).build();
                return new GenericNativeIndexProvider( context, dir, collector, Config.defaults() );
            };

    NativeIndexProviderTest()
    {
        super( factory );
    }

    @Test
    void shouldNotCheckConflictsWhenApplyingUpdatesInOnlineAccessor() throws IOException, IndexEntryConflictException
    {
        // given
        Value someValue = Values.of( 1 );
        provider = newProvider();

        // when
        IndexDescriptor descriptor = descriptorUnique();
        try ( IndexAccessor accessor = provider.getOnlineAccessor( descriptor, samplingConfig(), tokenNameLookup );
              IndexUpdater indexUpdater = accessor.newUpdater( IndexUpdateMode.ONLINE, NULL ) )
        {
            indexUpdater.process( IndexEntryUpdate.add( 1, descriptor.schema(), someValue ) );

            // then
            // ... expect no failure on duplicate value
            indexUpdater.process( IndexEntryUpdate.add( 2, descriptor.schema(), someValue ) );
        }
    }

    private IndexDescriptor descriptorUnique()
    {
        return completeConfiguration( uniqueForSchema( forLabel( labelId, propId ), PROVIDER_DESCRIPTOR ).withName( "constraint" ).materialise( indexId ) );
    }

    @Override
    IndexDescriptor descriptor()
    {
        return completeConfiguration( forSchema( forLabel( labelId, propId ), PROVIDER_DESCRIPTOR ).withName( "index" ).materialise( indexId ) );
    }

    @Override
    IndexDescriptor otherDescriptor()
    {
        return completeConfiguration( forSchema( forLabel( labelId, propId ), PROVIDER_DESCRIPTOR ).withName( "otherIndex" ).materialise( indexId + 1 ) );
    }

    @Override
    IndexPrototype validPrototype()
    {
        return forSchema( forLabel( labelId, propId ), PROVIDER_DESCRIPTOR ).withName( "index" );
    }

    @Override
    List<IndexPrototype> invalidPrototypes()
    {
        return List.of(
                forSchema( forAnyEntityTokens( EntityType.NODE ) ).withName( "unsupported" ),
                forSchema( forLabel( labelId, propId ) ).withIndexType( IndexType.FULLTEXT ).withName( "unsupported" ),
                forSchema( forLabel( labelId, propId ), PROVIDER_DESCRIPTOR ).withIndexType( IndexType.LOOKUP ).withName( "unsupported" ) );
    }

    @Override
    void setupIndexFolders( FileSystemAbstraction fs ) throws IOException
    {
        Path nativeSchemaIndexStoreDirectory = newProvider().directoryStructure().rootDirectory();
        fs.mkdirs( nativeSchemaIndexStoreDirectory );
    }
}
