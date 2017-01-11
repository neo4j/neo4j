/**
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
package org.neo4j.kernel.api.impl.index;

import org.apache.lucene.store.Directory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.neo4j.kernel.api.exceptions.index.IndexCapacityExceededException;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.Reservation;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.asUniqueSet;
import static org.neo4j.helpers.collection.IteratorUtil.emptySetOf;
import static org.neo4j.kernel.api.impl.index.IndexWriterFactories.reserving;

public class LuceneIndexAccessorTest
{
    @Test
    public void indexReaderShouldHonorRepeatableReads() throws Exception
    {
        // GIVEN
        updateAndCommit( asList( add( nodeId, value ) ) );
        IndexReader reader = accessor.newReader();

        // WHEN
        updateAndCommit( asList( remove( nodeId, value ) ) );

        // THEN
        assertEquals( asSet( nodeId ), asUniqueSet( reader.lookup( value ) ) );
        reader.close();
    }

    @Test
    public void multipleIndexReadersFromDifferentPointsInTimeCanSeeDifferentResults() throws Exception
    {
        // WHEN
        updateAndCommit( asList( add( nodeId, value ) ) );
        IndexReader firstReader = accessor.newReader();
        updateAndCommit( asList( add( nodeId2, value2 ) ) );
        IndexReader secondReader = accessor.newReader();

        // THEN
        assertEquals( asSet( nodeId ), asUniqueSet( firstReader.lookup( value ) ) );
        assertEquals( asSet(  ), asUniqueSet( firstReader.lookup( value2 ) ) );
        assertEquals( asSet( nodeId ), asUniqueSet( secondReader.lookup( value ) ) );
        assertEquals( asSet( nodeId2 ), asUniqueSet( secondReader.lookup( value2 ) ) );
        firstReader.close();
        secondReader.close();
    }

    @Test
    public void canAddNewData() throws Exception
    {
        // WHEN
        updateAndCommit( asList(
                add( nodeId, value ),
                add( nodeId2, value2 ) ) );
        IndexReader reader = accessor.newReader();

        // THEN
        assertEquals( asSet( nodeId ), asUniqueSet( reader.lookup( value ) ) );
        reader.close();
    }

    @Test
    public void canChangeExistingData() throws Exception
    {
        // GIVEN
        updateAndCommit( asList( add( nodeId, value ) ) );

        // WHEN
        updateAndCommit( asList( change( nodeId, value, value2 ) ) );
        IndexReader reader = accessor.newReader();

        // THEN
        assertEquals( asSet( nodeId ), asUniqueSet( reader.lookup( value2 ) ) );
        assertEquals( emptySetOf( Long.class ), asUniqueSet( reader.lookup( value ) ) );
        reader.close();
    }

    @Test
    public void canRemoveExistingData() throws Exception
    {
        // GIVEN
        updateAndCommit( asList(
                add( nodeId, value ),
                add( nodeId2, value2 ) ) );

        // WHEN
        updateAndCommit( asList( remove( nodeId, value ) ) );
        IndexReader reader = accessor.newReader();

        // THEN
        assertEquals( asSet( nodeId2 ), asUniqueSet( reader.lookup( value2 ) ) );
        assertEquals( asSet(  ), asUniqueSet( reader.lookup( value ) ) );
        reader.close();
    }

    @Test
    public void reservationShouldAllowReleaseOnlyOnce() throws Exception
    {
        // Given
        IndexUpdater updater = newIndexUpdater( mock( ReservingLuceneIndexWriter.class ) );

        Reservation reservation = updater.validate( asList( add( 1, "foo" ), add( 2, "bar" ) ) );
        reservation.release();

        try
        {
            // When
            reservation.release();
            fail( "Should have thrown " + IllegalStateException.class.getSimpleName() );
        }
        catch ( IllegalStateException e )
        {
            // Then
            assertThat( e.getMessage(), startsWith( "Reservation was already released" ) );
        }
    }

    @Test
    public void updaterShouldReserveDocumentsForAdditions() throws Exception
    {
        // Given
        ReservingLuceneIndexWriter indexWriter = mock( ReservingLuceneIndexWriter.class );
        IndexUpdater updater = newIndexUpdater( indexWriter );

        // When
        updater.validate( asList( add( 1, "value1" ), add( 2, "value2" ), add( 3, "value3" ) ) );

        // Then
        verify( indexWriter ).reserveInsertions( 3 );
    }


    @Test
    public void updaterShouldReserveDocumentsForUpdates() throws Exception
    {
        // Given
        ReservingLuceneIndexWriter indexWriter = mock( ReservingLuceneIndexWriter.class );
        IndexUpdater updater = newIndexUpdater( indexWriter );

        // When
        updater.validate( asList( change( 1, "before1", "after1" ), change( 2, "before2", "after2" ) ) );

        // Then
        verify( indexWriter ).reserveInsertions( 2 );
    }

    @Test
    public void updaterShouldNotReserveDocumentsForDeletions() throws Exception
    {
        // Given
        ReservingLuceneIndexWriter indexWriter = mock( ReservingLuceneIndexWriter.class );
        IndexUpdater updater = newIndexUpdater( indexWriter );

        // When
        updater.validate( asList( remove( 1, "value1" ), remove( 2, "value2" ), remove( 3, "value3" ) ) );

        // Then
        verify( indexWriter ).reserveInsertions( 0 );
    }

    @Test
    public void updaterShouldReserveDocuments() throws Exception
    {
        // Given
        ReservingLuceneIndexWriter indexWriter = mock( ReservingLuceneIndexWriter.class );
        IndexUpdater updater = newIndexUpdater( indexWriter );

        // When
        updater.validate( asList(
                add( 1, "foo" ),
                add( 2, "bar" ),
                add( 3, "baz" ) ) );

        updater.validate( asList(
                change( 1, "foo1", "bar1" ),
                add( 2, "foo2" ),
                add( 3, "foo3" ),
                change( 4, "foo4", "bar4" ),
                remove( 5, "foo5" ) ) );

        updater.validate( asList(
                change( 1, "foo1", "bar1" ),
                remove( 2, "foo2" ),
                remove( 3, "foo3" ) ) );

        // Then
        InOrder order = inOrder( indexWriter );
        order.verify( indexWriter ).createSearcherManager();
        order.verify( indexWriter ).reserveInsertions( 3 );
        order.verify( indexWriter ).reserveInsertions( 4 );
        order.verify( indexWriter ).reserveInsertions( 1 );
        verifyNoMoreInteractions( indexWriter );
    }

    @Test
    public void updaterShouldReturnCorrectReservation() throws Exception
    {
        // Given
        ReservingLuceneIndexWriter indexWriter = mock( ReservingLuceneIndexWriter.class );
        IndexUpdater updater = newIndexUpdater( indexWriter );

        // When
        Reservation reservation = updater.validate( asList(
                add( 1, "foo" ),
                change( 2, "bar", "baz" ),
                remove( 3, "qux" ) ) );

        reservation.release();

        // Then
        InOrder inOrder = inOrder( indexWriter );
        inOrder.verify( indexWriter ).reserveInsertions( 2 );
        inOrder.verify( indexWriter ).removeReservedInsertions( 2 );
    }

    private final long nodeId = 1, nodeId2 = 2;
    private final Object value = "value", value2 = 40;
    private final LuceneDocumentStructure documentLogic = new LuceneDocumentStructure();
    private final IndexWriterStatus writerLogic = new IndexWriterStatus();
    private final File dir = new File( "dir" );
    private LuceneIndexAccessor accessor;
    private DirectoryFactory.InMemoryDirectoryFactory dirFactory;

    @Before
    public void before() throws Exception
    {
        dirFactory = new DirectoryFactory.InMemoryDirectoryFactory();
        accessor = new NonUniqueLuceneIndexAccessor( documentLogic, reserving(), writerLogic, dirFactory, dir );
    }

    @After
    public void after()
    {
        dirFactory.close();
    }

    private NodePropertyUpdate add( long nodeId, Object value )
    {
        return NodePropertyUpdate.add( nodeId, 0, value, new long[0] );
    }

    private NodePropertyUpdate remove( long nodeId, Object value )
    {
        return NodePropertyUpdate.remove( nodeId, 0, value, new long[0] );
    }

    private NodePropertyUpdate change( long nodeId, Object valueBefore, Object valueAfter )
    {
        return NodePropertyUpdate.change( nodeId, 0, valueBefore, new long[0], valueAfter, new long[0] );
    }

    private void updateAndCommit( List<NodePropertyUpdate> nodePropertyUpdates )
            throws IOException, IndexEntryConflictException, IndexCapacityExceededException
    {
        try ( IndexUpdater updater = accessor.newUpdater( IndexUpdateMode.ONLINE ) )
        {
            for ( NodePropertyUpdate update : nodePropertyUpdates )
            {
                updater.process( update );
            }
        }
    }

    @SuppressWarnings( "unchecked" )
    private IndexUpdater newIndexUpdater( ReservingLuceneIndexWriter indexWriter ) throws IOException
    {
        IndexWriterFactory<ReservingLuceneIndexWriter> indexWriterFactory = mock( IndexWriterFactory.class );
        when( indexWriterFactory.create( any( Directory.class ) ) ).thenReturn( indexWriter );

        LuceneIndexAccessor indexAccessor = new NonUniqueLuceneIndexAccessor( documentLogic, indexWriterFactory,
                writerLogic, dirFactory, dir );

        return indexAccessor.newUpdater( IndexUpdateMode.ONLINE );
    }
}
