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
package org.neo4j.kernel.api.impl.schema;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.index.storage.IndexStorageFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.test.extension.EphemeralNeo4jLayoutExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.values.storable.Values;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.SchemaIndex.NATIVE30;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesBySubProvider;

@EphemeralNeo4jLayoutExtension
class AccessUniqueDatabaseIndexTest
{
    @Inject
    private EphemeralFileSystemAbstraction fileSystem;
    @Inject
    private DatabaseLayout databaseLayout;
    private final DirectoryFactory directoryFactory = new DirectoryFactory.InMemoryDirectoryFactory();
    private final IndexDescriptor index = IndexPrototype.uniqueForSchema( SchemaDescriptor.forLabel( 1000, 100 ) ).withName( "a" ).materialise( 0 );

    @Test
    void shouldAddUniqueEntries() throws Exception
    {
        // given
        PartitionedIndexStorage indexStorage = getIndexStorage();
        LuceneIndexAccessor accessor = createAccessor( indexStorage );

        // when
        updateAndCommit( accessor, asList( add( 1L, "value1" ), add( 2L, "value2" ) ) );
        updateAndCommit( accessor, singletonList( add( 3L, "value3" ) ) );
        accessor.close();

        // then
        assertEquals( singletonList( 1L ), getAllNodes( indexStorage, "value1" ) );
    }

    @Test
    void shouldUpdateUniqueEntries() throws Exception
    {
        // given
        PartitionedIndexStorage indexStorage = getIndexStorage();

        LuceneIndexAccessor accessor = createAccessor( indexStorage );

        // when
        updateAndCommit( accessor, singletonList( add( 1L, "value1" ) ) );
        updateAndCommit( accessor, singletonList( change( 1L, "value1", "value2" ) ) );
        accessor.close();

        // then
        assertEquals( singletonList( 1L ), getAllNodes( indexStorage, "value2" ) );
        assertEquals( emptyList(), getAllNodes( indexStorage, "value1" ) );
    }

    @Test
    void shouldRemoveAndAddEntries() throws Exception
    {
        // given
        PartitionedIndexStorage indexStorage = getIndexStorage();

        LuceneIndexAccessor accessor = createAccessor( indexStorage );

        // when
        updateAndCommit( accessor, singletonList( add( 1L, "value1" ) ) );
        updateAndCommit( accessor, singletonList( add( 2L, "value2" ) ) );
        updateAndCommit( accessor, singletonList( add( 3L, "value3" ) ) );
        updateAndCommit( accessor, singletonList( add( 4L, "value4" ) ) );
        updateAndCommit( accessor, singletonList( remove( 1L, "value1" ) ) );
        updateAndCommit( accessor, singletonList( remove( 2L, "value2" ) ) );
        updateAndCommit( accessor, singletonList( remove( 3L, "value3" ) ) );
        updateAndCommit( accessor, singletonList( add( 1L, "value1" ) ) );
        updateAndCommit( accessor, singletonList( add( 3L, "value3b" ) ) );
        accessor.close();

        // then
        assertEquals( singletonList( 1L ), getAllNodes( indexStorage, "value1" ) );
        assertEquals( emptyList(), getAllNodes( indexStorage, "value2" ) );
        assertEquals( emptyList(), getAllNodes( indexStorage, "value3" ) );
        assertEquals( singletonList( 3L ), getAllNodes( indexStorage, "value3b" ) );
        assertEquals( singletonList( 4L ), getAllNodes( indexStorage, "value4" ) );
    }

    @Test
    void shouldConsiderWholeTransactionForValidatingUniqueness() throws Exception
    {
        // given
        PartitionedIndexStorage indexStorage = getIndexStorage();

        LuceneIndexAccessor accessor = createAccessor( indexStorage );

        // when
        updateAndCommit( accessor, singletonList( add( 1L, "value1" ) ) );
        updateAndCommit( accessor, singletonList( add( 2L, "value2" ) ) );
        updateAndCommit( accessor, asList( change( 1L, "value1", "value2" ), change( 2L, "value2", "value1" ) ) );
        accessor.close();

        // then
        assertEquals( singletonList( 2L ), getAllNodes( indexStorage, "value1" ) );
        assertEquals( singletonList( 1L ), getAllNodes( indexStorage, "value2" ) );
    }

    private LuceneIndexAccessor createAccessor( PartitionedIndexStorage indexStorage ) throws IOException
    {
        SchemaIndex luceneIndex = LuceneSchemaIndexBuilder.create( index, Config.defaults() )
                .withIndexStorage( indexStorage )
                .build();
        luceneIndex.open();
        return new LuceneIndexAccessor( luceneIndex, index );
    }

    private PartitionedIndexStorage getIndexStorage()
    {
        IndexProviderDescriptor descriptor = new IndexProviderDescriptor( NATIVE30.providerKey(), NATIVE30.providerVersion() );
        IndexDirectoryStructure parent = directoriesByProvider( databaseLayout.databaseDirectory() ).forProvider( descriptor );
        IndexStorageFactory storageFactory = new IndexStorageFactory( directoryFactory, fileSystem,
                directoriesBySubProvider( parent ).forProvider( LuceneIndexProvider.DESCRIPTOR ) );
        return storageFactory.indexStorageOf( 1 );
    }

    private IndexEntryUpdate<?> add( long nodeId, Object propertyValue )
    {
        return IndexEntryUpdate.add( nodeId, index.schema(), Values.of( propertyValue ) );
    }

    private IndexEntryUpdate<?> change( long nodeId, Object oldValue, Object newValue )
    {
        return IndexEntryUpdate.change( nodeId, index.schema(), Values.of( oldValue ), Values.of( newValue ) );
    }

    private IndexEntryUpdate<?> remove( long nodeId, Object oldValue )
    {
        return IndexEntryUpdate.remove( nodeId, index.schema(), Values.of( oldValue ) );
    }

    private List<Long> getAllNodes( PartitionedIndexStorage indexStorage, String propertyValue ) throws IOException
    {
        return AllNodesCollector.getAllNodes( indexStorage.openDirectory( indexStorage.getPartitionFolder( 1 ) ),
                Values.stringValue( propertyValue ) );
    }

    private void updateAndCommit( IndexAccessor accessor, Iterable<IndexEntryUpdate<?>> updates ) throws IndexEntryConflictException
    {
        try ( IndexUpdater updater = accessor.newUpdater( IndexUpdateMode.ONLINE ) )
        {
            for ( IndexEntryUpdate<?> update : updates )
            {
                updater.process( update );
            }
        }
    }
}
