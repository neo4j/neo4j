/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.api.impl.schema;

import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.internal.helpers.collection.BoundedIterable;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.io.memory.ByteBufferFactory;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.values.storable.TextValue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.kernel.api.impl.index.storage.DirectoryFactory.PERSISTENT;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.kernel.api.index.IndexProvider.Monitor.EMPTY;
import static org.neo4j.kernel.impl.api.index.PhaseTracker.nullInstance;
import static org.neo4j.storageengine.api.IndexEntryUpdate.add;
import static org.neo4j.values.storable.Values.stringValue;

@TestDirectoryExtension
public class LuceneIndexAccessorIT
{
    @Inject
    private TestDirectory directory;

    private final LifeSupport life = new LifeSupport();
    private final IndexDescriptor indexDescriptor = IndexPrototype.forSchema( SchemaDescriptor.forLabel( 1, 2 ) ).withName( "TestIndex" ).materialise( 99 );
    private LuceneIndexProvider indexProvider;
    private IndexSamplingConfig samplingConfig;

    @BeforeEach
    void setUp()
    {
        File path = directory.directory( "db" );
        Config config = Config.defaults();
        indexProvider = new LuceneIndexProvider( directory.getFileSystem(), PERSISTENT, directoriesByProvider( path ), EMPTY, config, true );
        life.add( indexProvider );
        life.start();
        samplingConfig = new IndexSamplingConfig( config );
    }

    @AfterEach
    void close()
    {
        life.shutdown();
    }

    @Test
    void shouldIterateAllDocumentsEvenWhenContainingDeletions() throws Exception
    {
        // given
        int nodes = 100;
        MutableLongSet expectedNodes = LongSets.mutable.empty();
        TokenNameLookup tokenNameLookup = mock( TokenNameLookup.class );
        populateWithInitialNodes( nodes, expectedNodes, tokenNameLookup );
        try ( IndexAccessor accessor = indexProvider.getOnlineAccessor( indexDescriptor, samplingConfig, tokenNameLookup ) )
        {
            // when
            removeSomeNodes( nodes / 2, accessor, expectedNodes );

            // then
            try ( BoundedIterable<Long> reader = accessor.newAllEntriesReader() )
            {
                MutableLongSet readIds = LongSets.mutable.empty();
                reader.forEach( readIds::add );
                assertEquals( expectedNodes, readIds );
            }
        }
    }

    private void removeSomeNodes( int nodes, IndexAccessor accessor, MutableLongSet expectedNodes ) throws IndexEntryConflictException
    {
        try ( IndexUpdater updater = accessor.newUpdater( IndexUpdateMode.ONLINE ) )
        {
            for ( long id = 0; id < nodes; id++ )
            {
                updater.process( IndexEntryUpdate.remove( id, indexDescriptor, value( id ) ) );
                expectedNodes.remove( id );
            }
        }
    }

    private void populateWithInitialNodes( int nodes, MutableLongSet expectedNodes, TokenNameLookup tokenNameLookup ) throws IndexEntryConflictException
    {
        IndexPopulator populator =
                indexProvider.getPopulator( indexDescriptor, samplingConfig, ByteBufferFactory.heapBufferFactory( (int) kibiBytes( 100 ) ), tokenNameLookup );
        Collection<IndexEntryUpdate<IndexDescriptor>> initialData = new ArrayList<>();
        for ( long id = 0; id < nodes; id++ )
        {
            initialData.add( add( id, indexDescriptor, value( id ) ) );
            expectedNodes.add( id );
        }
        populator.create();
        populator.add( initialData );
        populator.scanCompleted( nullInstance );
        populator.close( true );
    }

    private TextValue value( long id )
    {
        return stringValue( "string_" + id );
    }
}
