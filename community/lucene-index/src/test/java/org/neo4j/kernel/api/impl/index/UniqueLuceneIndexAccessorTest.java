/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.index;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.neo4j.kernel.api.exceptions.index.IndexCapacityExceededException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.IteratorUtil.emptyListOf;
import static org.neo4j.kernel.api.impl.index.IndexWriterFactories.reserving;

public class UniqueLuceneIndexAccessorTest
{
    private final DirectoryFactory directoryFactory = new DirectoryFactory.InMemoryDirectoryFactory();
    private final File indexDirectory = new File( "index1" );

    @Test
    public void shouldAddUniqueEntries() throws Exception
    {
        // given
        UniqueLuceneIndexAccessor accessor = createAccessor();

        // when
        updateAndCommit( accessor,  asList( add( 1l, "value1" ), add( 2l, "value2" ) ) );
        updateAndCommit( accessor,  asList( add( 3l, "value3" ) ) );
        accessor.close();

        // then
        assertEquals( asList( 1l ), getAllNodes( "value1" ) );
    }

    @Test
    public void shouldUpdateUniqueEntries() throws Exception
    {
        // given
        UniqueLuceneIndexAccessor accessor = createAccessor();

        // when
        updateAndCommit( accessor,  asList( add( 1l, "value1" ) ) );
        updateAndCommit( accessor,  asList( change( 1l, "value1", "value2" ) ) );
        accessor.close();

        // then
        assertEquals( asList( 1l ), getAllNodes( "value2" ) );
        assertEquals( emptyListOf( Long.class ), getAllNodes( "value1" ) );
    }

    @Test
    public void shouldRemoveAndAddEntries() throws Exception
    {
        // given
        UniqueLuceneIndexAccessor accessor = createAccessor();

        // when
        updateAndCommit( accessor,  asList( add( 1l, "value1" ) ) );
        updateAndCommit( accessor,  asList( add( 2l, "value2" ) ) );
        updateAndCommit( accessor,  asList( add( 3l, "value3" ) ) );
        updateAndCommit( accessor,  asList( add( 4l, "value4" ) ) );
        updateAndCommit( accessor,  asList( remove( 1l, "value1" ) ) );
        updateAndCommit( accessor,  asList( remove( 2l, "value2" ) ) );
        updateAndCommit( accessor,  asList( remove( 3l, "value3" ) ) );
        updateAndCommit( accessor,  asList( add( 1l, "value1" ) ) );
        updateAndCommit( accessor,  asList( add( 3l, "value3b" ) ) );
        accessor.close();

        // then
        assertEquals( asList( 1l ), getAllNodes( "value1" ) );
        assertEquals( emptyListOf( Long.class ), getAllNodes( "value2" ) );
        assertEquals( emptyListOf( Long.class ), getAllNodes( "value3" ) );
        assertEquals( asList( 3l ), getAllNodes( "value3b" ) );
        assertEquals( asList( 4l ), getAllNodes( "value4" ) );
    }

    @Test
    public void shouldConsiderWholeTransactionForValidatingUniqueness() throws Exception
    {
        // given
        UniqueLuceneIndexAccessor accessor = createAccessor();

        // when
        updateAndCommit( accessor,  asList( add( 1l, "value1" ) ) );
        updateAndCommit( accessor,  asList( add( 2l, "value2" ) ) );
        updateAndCommit( accessor,  asList( change( 1l, "value1", "value2" ), change( 2l, "value2", "value1" ) ) );
        accessor.close();

        // then
        assertEquals( asList( 2l ), getAllNodes( "value1" ) );
        assertEquals( asList( 1l ), getAllNodes( "value2" ) );
    }

    private UniqueLuceneIndexAccessor createAccessor() throws IOException
    {
        return new UniqueLuceneIndexAccessor( new LuceneDocumentStructure(), false, reserving(),
                directoryFactory, indexDirectory );
    }

    private NodePropertyUpdate add( long nodeId, Object propertyValue )
    {
        return NodePropertyUpdate.add( nodeId, 100, propertyValue, new long[]{1000} );
    }

    private NodePropertyUpdate change( long nodeId, Object oldValue, Object newValue )
    {
        return NodePropertyUpdate.change( nodeId, 100, oldValue, new long[]{1000}, newValue, new long[]{1000} );
    }

    private NodePropertyUpdate remove( long nodeId, Object oldValue)
    {
        return NodePropertyUpdate.remove( nodeId, 100, oldValue, new long[]{1000} );
    }

    private List<Long> getAllNodes( String propertyValue ) throws IOException
    {
        return AllNodesCollector.getAllNodes( directoryFactory, indexDirectory, propertyValue );
    }
    
    private void updateAndCommit( IndexAccessor accessor, Iterable<NodePropertyUpdate> updates )
            throws IOException, IndexEntryConflictException, IndexCapacityExceededException
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
