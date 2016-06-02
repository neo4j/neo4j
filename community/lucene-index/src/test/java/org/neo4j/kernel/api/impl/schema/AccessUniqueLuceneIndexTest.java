/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.io.IOException;
import java.util.List;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.index.storage.IndexStorageFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.test.TargetDirectory;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;

public class AccessUniqueLuceneIndexTest
{
    @Rule
    public TargetDirectory.TestDirectory testDirectory = TargetDirectory.testDirForTest( getClass() );
    private final DirectoryFactory directoryFactory = DirectoryFactory.PERSISTENT;


    @Test
    public void shouldAddUniqueEntries() throws Exception
    {
        // given
        PartitionedIndexStorage indexStorage = getIndexStorage();
        LuceneIndexAccessor accessor = createAccessor( indexStorage );

        // when
        updateAndCommit( accessor, asList( add( 1l, "value1" ), add( 2l, "value2" ) ) );
        updateAndCommit( accessor, asList( add( 3l, "value3" ) ) );
        accessor.close();

        // then
        assertEquals( asList( 1l ), getAllNodes( indexStorage, "value1" ) );
    }

    @Test
    public void shouldUpdateUniqueEntries() throws Exception
    {
        // given
        PartitionedIndexStorage indexStorage = getIndexStorage();

        LuceneIndexAccessor accessor = createAccessor( indexStorage );

        // when
        updateAndCommit( accessor, asList( add( 1l, "value1" ) ) );
        updateAndCommit( accessor, asList( change( 1l, "value1", "value2" ) ) );
        accessor.close();

        // then
        assertEquals( asList( 1l ), getAllNodes( indexStorage, "value2" ) );
        assertEquals( emptyList(), getAllNodes( indexStorage, "value1" ) );
    }

    @Test
    public void shouldRemoveAndAddEntries() throws Exception
    {
        // given
        PartitionedIndexStorage indexStorage = getIndexStorage();

        LuceneIndexAccessor accessor = createAccessor( indexStorage );

        // when
        updateAndCommit( accessor, asList( add( 1l, "value1" ) ) );
        updateAndCommit( accessor, asList( add( 2l, "value2" ) ) );
        updateAndCommit( accessor, asList( add( 3l, "value3" ) ) );
        updateAndCommit( accessor, asList( add( 4l, "value4" ) ) );
        updateAndCommit( accessor, asList( remove( 1l, "value1" ) ) );
        updateAndCommit( accessor, asList( remove( 2l, "value2" ) ) );
        updateAndCommit( accessor, asList( remove( 3l, "value3" ) ) );
        updateAndCommit( accessor, asList( add( 1l, "value1" ) ) );
        updateAndCommit( accessor, asList( add( 3l, "value3b" ) ) );
        accessor.close();

        // then
        assertEquals( asList( 1l ), getAllNodes( indexStorage, "value1" ) );
        assertEquals( emptyList(), getAllNodes( indexStorage, "value2" ) );
        assertEquals( emptyList(), getAllNodes( indexStorage, "value3" ) );
        assertEquals( asList( 3l ), getAllNodes( indexStorage, "value3b" ) );
        assertEquals( asList( 4l ), getAllNodes( indexStorage, "value4" ) );
    }

    @Test
    public void shouldConsiderWholeTransactionForValidatingUniqueness() throws Exception
    {
        // given
        PartitionedIndexStorage indexStorage = getIndexStorage();

        LuceneIndexAccessor accessor = createAccessor( indexStorage );

        // when
        updateAndCommit( accessor, asList( add( 1l, "value1" ) ) );
        updateAndCommit( accessor, asList( add( 2l, "value2" ) ) );
        updateAndCommit( accessor, asList( change( 1l, "value1", "value2" ), change( 2l, "value2", "value1" ) ) );
        accessor.close();

        // then
        assertEquals( asList( 2l ), getAllNodes( indexStorage, "value1" ) );
        assertEquals( asList( 1l ), getAllNodes( indexStorage, "value2" ) );
    }

    private LuceneIndexAccessor createAccessor( PartitionedIndexStorage indexStorage ) throws IOException
    {
        LuceneSchemaIndex luceneIndex = LuceneSchemaIndexBuilder.create()
                .withIndexStorage( indexStorage )
                .uniqueIndex()
                .build();
        luceneIndex.create();
        luceneIndex.open();
        return new LuceneIndexAccessor( luceneIndex );
    }

    private PartitionedIndexStorage getIndexStorage() throws IOException
    {
        IndexStorageFactory storageFactory = new IndexStorageFactory( directoryFactory, new DefaultFileSystemAbstraction(),
                testDirectory.graphDbDir() );
        return storageFactory.indexStorageOf( 1 );
    }

    private NodePropertyUpdate add( long nodeId, Object propertyValue )
    {
        return NodePropertyUpdate.add( nodeId, 100, propertyValue, new long[]{1000} );
    }

    private NodePropertyUpdate change( long nodeId, Object oldValue, Object newValue )
    {
        return NodePropertyUpdate.change( nodeId, 100, oldValue, new long[]{1000}, newValue, new long[]{1000} );
    }

    private NodePropertyUpdate remove( long nodeId, Object oldValue )
    {
        return NodePropertyUpdate.remove( nodeId, 100, oldValue, new long[]{1000} );
    }

    private List<Long> getAllNodes( PartitionedIndexStorage indexStorage, String propertyValue ) throws IOException
    {
        return AllNodesCollector.getAllNodes( indexStorage.openDirectory( indexStorage.getPartitionFolder( 1 ) ),
                propertyValue );
    }

    private void updateAndCommit( IndexAccessor accessor, Iterable<NodePropertyUpdate> updates )
            throws IOException, IndexEntryConflictException
    {
        try ( IndexUpdater updater = accessor.newUpdater( IndexUpdateMode.ONLINE ) )
        {
            for ( NodePropertyUpdate update : updates )
            {
                updater.process( update );
            }
        }
    }
}
