/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.index.storage.IndexStorageFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexQueryHelper;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;
import org.neo4j.values.storable.Values;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

import static org.neo4j.kernel.api.impl.schema.LuceneSchemaIndexProviderFactory.PROVIDER_DESCRIPTOR;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProviderKey;

import static org.junit.Assert.assertEquals;

public class AccessUniqueDatabaseIndexTest
{
    @Rule
    public final EphemeralFileSystemRule fileSystemRule = new EphemeralFileSystemRule();
    private final DirectoryFactory directoryFactory = new DirectoryFactory.InMemoryDirectoryFactory();
    private final File storeDirectory = new File( "db" );
    private final IndexDescriptor index = IndexDescriptorFactory.uniqueForLabel( 1000, 100 );

    @Test
    public void shouldAddUniqueEntries() throws Exception
    {
        // given
        PartitionedIndexStorage indexStorage = getIndexStorage();
        LuceneIndexAccessor accessor = createAccessor( indexStorage );

        // when
        updateAndCommit( accessor, asList( add( 1L, "value1" ), add( 2L, "value2" ) ) );
        updateAndCommit( accessor, asList( add( 3L, "value3" ) ) );
        accessor.close();

        // then
        assertEquals( asList( 1L ), getAllNodes( indexStorage, "value1" ) );
    }

    @Test
    public void shouldUpdateUniqueEntries() throws Exception
    {
        // given
        PartitionedIndexStorage indexStorage = getIndexStorage();

        LuceneIndexAccessor accessor = createAccessor( indexStorage );

        // when
        updateAndCommit( accessor, asList( add( 1L, "value1" ) ) );
        updateAndCommit( accessor, asList( change( 1L, "value1", "value2" ) ) );
        accessor.close();

        // then
        assertEquals( asList( 1L ), getAllNodes( indexStorage, "value2" ) );
        assertEquals( emptyList(), getAllNodes( indexStorage, "value1" ) );
    }

    @Test
    public void shouldRemoveAndAddEntries() throws Exception
    {
        // given
        PartitionedIndexStorage indexStorage = getIndexStorage();

        LuceneIndexAccessor accessor = createAccessor( indexStorage );

        // when
        updateAndCommit( accessor, asList( add( 1L, "value1" ) ) );
        updateAndCommit( accessor, asList( add( 2L, "value2" ) ) );
        updateAndCommit( accessor, asList( add( 3L, "value3" ) ) );
        updateAndCommit( accessor, asList( add( 4L, "value4" ) ) );
        updateAndCommit( accessor, asList( remove( 1L, "value1" ) ) );
        updateAndCommit( accessor, asList( remove( 2L, "value2" ) ) );
        updateAndCommit( accessor, asList( remove( 3L, "value3" ) ) );
        updateAndCommit( accessor, asList( add( 1L, "value1" ) ) );
        updateAndCommit( accessor, asList( add( 3L, "value3b" ) ) );
        accessor.close();

        // then
        assertEquals( asList( 1L ), getAllNodes( indexStorage, "value1" ) );
        assertEquals( emptyList(), getAllNodes( indexStorage, "value2" ) );
        assertEquals( emptyList(), getAllNodes( indexStorage, "value3" ) );
        assertEquals( asList( 3L ), getAllNodes( indexStorage, "value3b" ) );
        assertEquals( asList( 4L ), getAllNodes( indexStorage, "value4" ) );
    }

    @Test
    public void shouldConsiderWholeTransactionForValidatingUniqueness() throws Exception
    {
        // given
        PartitionedIndexStorage indexStorage = getIndexStorage();

        LuceneIndexAccessor accessor = createAccessor( indexStorage );

        // when
        updateAndCommit( accessor, asList( add( 1L, "value1" ) ) );
        updateAndCommit( accessor, asList( add( 2L, "value2" ) ) );
        updateAndCommit( accessor, asList( change( 1L, "value1", "value2" ), change( 2L, "value2", "value1" ) ) );
        accessor.close();

        // then
        assertEquals( asList( 2L ), getAllNodes( indexStorage, "value1" ) );
        assertEquals( asList( 1L ), getAllNodes( indexStorage, "value2" ) );
    }

    private LuceneIndexAccessor createAccessor( PartitionedIndexStorage indexStorage ) throws IOException
    {
        SchemaIndex luceneIndex = LuceneSchemaIndexBuilder.create( index )
                .withIndexStorage( indexStorage )
                .build();
        luceneIndex.open();
        return new LuceneIndexAccessor( luceneIndex, index );
    }

    private PartitionedIndexStorage getIndexStorage()
    {
        IndexStorageFactory storageFactory = new IndexStorageFactory( directoryFactory, fileSystemRule.get(),
                directoriesByProviderKey( storeDirectory ).forProvider( PROVIDER_DESCRIPTOR ) );
        return storageFactory.indexStorageOf( 1, false );
    }

    private IndexEntryUpdate<?> add( long nodeId, Object propertyValue )
    {
        return IndexQueryHelper.add( nodeId, index.schema(), propertyValue );
    }

    private IndexEntryUpdate<?> change( long nodeId, Object oldValue, Object newValue )
    {
        return IndexQueryHelper.change( nodeId, index.schema(), oldValue, newValue );
    }

    private IndexEntryUpdate<?> remove( long nodeId, Object oldValue )
    {
        return IndexQueryHelper.remove( nodeId, index.schema(), oldValue );
    }

    private List<Long> getAllNodes( PartitionedIndexStorage indexStorage, String propertyValue ) throws IOException
    {
        return AllNodesCollector.getAllNodes( indexStorage.openDirectory( indexStorage.getPartitionFolder( 1 ) ),
                Values.stringValue( propertyValue ) );
    }

    private void updateAndCommit( IndexAccessor accessor, Iterable<IndexEntryUpdate<?>> updates )
            throws IOException, IndexEntryConflictException
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
