/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.junit.Test;

import org.neo4j.kernel.api.index.DuplicateIndexEntryConflictException;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.PreexistingIndexEntryConflictException;

import static java.util.Arrays.asList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.emptyListOf;
import static org.neo4j.kernel.api.impl.index.IndexWriterFactories.standard;

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
        accessor.updateAndCommit( asList( add( 1l, "value1" ), add( 2l, "value2" ) ) );
        accessor.updateAndCommit( asList( add( 3l, "value3" ) ) );
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
        accessor.updateAndCommit( asList( add( 1l, "value1" ) ) );
        accessor.updateAndCommit( asList( change( 1l, "value1", "value2" ) ) );
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
        accessor.updateAndCommit( asList( add( 1l, "value1" ) ) );
        accessor.updateAndCommit( asList( add( 2l, "value2" ) ) );
        accessor.updateAndCommit( asList( add( 3l, "value3" ) ) );
        accessor.updateAndCommit( asList( add( 4l, "value4" ) ) );
        accessor.updateAndCommit( asList( remove( 1l, "value1" ) ) );
        accessor.updateAndCommit( asList( remove( 2l, "value2" ) ) );
        accessor.updateAndCommit( asList( remove( 3l, "value3" ) ) );
        accessor.updateAndCommit( asList( add( 1l, "value1" ) ) );
        accessor.updateAndCommit( asList( add( 3l, "value3b" ) ) );
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
        accessor.updateAndCommit( asList( add( 1l, "value1" ) ) );
        accessor.updateAndCommit( asList( add( 2l, "value2" ) ) );
        accessor.updateAndCommit( asList( change( 1l, "value1", "value2" ), change( 2l, "value2", "value1" ) ) );
        accessor.close();

        // then
        assertEquals( asList( 2l ), getAllNodes( "value1" ) );
        assertEquals( asList( 1l ), getAllNodes( "value2" ) );
    }

    @Test
    public void shouldRejectChangingEntryToAlreadyIndexedValue() throws Exception
    {
        // given
        UniqueLuceneIndexAccessor accessor = createAccessor();

        accessor.updateAndCommit( asList( add( 1l, "value1" ) ) );
        accessor.updateAndCommit( asList( add( 2l, "value2" ) ) );

        // when
        try
        {
            accessor.updateAndCommit( asList( change( 1l, "value1", "value2" ) ) );

            fail( "expected exception" );
        }
        // then
        catch ( PreexistingIndexEntryConflictException conflict )
        {
            assertConflict( conflict, "value2", 2l, 1l );
        }
    }

    @Test
    public void shouldRejectAddingEntryToValueAlreadyIndexedByPriorChange() throws Exception
    {
        // given
        UniqueLuceneIndexAccessor accessor = createAccessor();

        accessor.updateAndCommit( asList( add( 1l, "value1" ) ) );
        accessor.updateAndCommit( asList( change( 1l, "value1", "value2" ) ) );

        // when
        try
        {
            accessor.updateAndCommit( asList( add( 2l, "value2" ) ) );

            fail( "expected exception" );
        }
        // then
        catch ( PreexistingIndexEntryConflictException conflict )
        {
            assertConflict( conflict, "value2", 1l, 2l );
        }
    }

    @Test
    public void shouldRejectEntryWithAlreadyIndexedValue() throws Exception
    {
        // given
        UniqueLuceneIndexAccessor accessor = createAccessor();

        accessor.updateAndCommit( asList( add( 1l, "value1" ) ) );

        // when
        try
        {
            accessor.updateAndCommit( asList( add( 2l, "value1" ) ) );

            fail( "expected exception" );
        }
        // then
        catch ( PreexistingIndexEntryConflictException conflict )
        {
            assertConflict( conflict, "value1", 1l, 2l );
        }
    }

    @Test
    public void shouldRejectEntriesInSameTransactionWithDuplicatedIndexedValues() throws Exception
    {
        // given
        UniqueLuceneIndexAccessor accessor = createAccessor();

        // when
        try
        {
            accessor.updateAndCommit( asList( add( 1l, "value1" ),
                    add( 2l, "value1" ) ) );

            fail( "expected exception" );
        }
        // then
        catch ( DuplicateIndexEntryConflictException conflict )
        {
            assertConflict( conflict, "value1", 1l, 2l );
        }
    }

    private UniqueLuceneIndexAccessor createAccessor() throws IOException
    {
        return new UniqueLuceneIndexAccessor( new LuceneDocumentStructure(), standard(), new IndexWriterStatus(),
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

    private void assertConflict( PreexistingIndexEntryConflictException conflict, String propertyValue,
                                 long existingNode, long addedNode )
    {
        assertEquals( propertyValue, conflict.getPropertyValue() );
        assertEquals( existingNode, conflict.getExistingNodeId() );
        assertEquals( addedNode, conflict.getAddedNodeId() );
    }

    private void assertConflict( DuplicateIndexEntryConflictException conflict, String propertyValue, Long... nodes )
    {
        assertEquals( propertyValue, conflict.getPropertyValue() );
        assertEquals( asSet( nodes ), conflict.getConflictingNodeIds() );
    }
}
