/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.PreparedIndexUpdates;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
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
        updateAndCommit( asList( add( nodeId2, value ) ) );
        IndexReader secondReader = accessor.newReader();

        // THEN
        assertEquals( asSet( nodeId ), asUniqueSet( firstReader.lookup( value ) ) );
        assertEquals( asSet( nodeId, nodeId2 ), asUniqueSet( secondReader.lookup( value ) ) );
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
                add( nodeId2, value ) ) );

        // WHEN
        updateAndCommit( asList( remove( nodeId, value ) ) );
        IndexReader reader = accessor.newReader();

        // THEN
        assertEquals( asSet( nodeId2 ), asUniqueSet( reader.lookup( value ) ) );
        reader.close();
    }

    @Test
    public void updaterShouldReserveDocumentsForAdditions() throws Exception
    {
        // Given
        LuceneIndexWriter writer = mock( LuceneIndexWriter.class );
        IndexUpdater updater = newIndexUpdater( writer );

        // When
        updater.prepare( asList( add( 42, "42" ), add( 4242, "4242" ) ) );

        // Then
        verify( writer ).reserveDocumentInsertions( 2 );
    }

    @Test
    public void updaterShouldReserveDocumentsForUpdates() throws Exception
    {
        // Given
        LuceneIndexWriter writer = mock( LuceneIndexWriter.class );
        IndexUpdater updater = newIndexUpdater( writer );

        // When
        updater.prepare( asList( change( 1, "1", "2" ), change( 2, "foo", "bar" ) ) );

        // Then
        verify( writer ).reserveDocumentInsertions( 2 );
    }

    @Test
    public void updaterShouldNotReserveDocumentsForDeletions() throws Exception
    {
        // Given
        LuceneIndexWriter writer = mock( LuceneIndexWriter.class );
        IndexUpdater updater = newIndexUpdater( writer );

        // When
        updater.prepare( asList( remove( 1, "foo" ), remove( 2, "bar" ) ) );

        // Then
        verify( writer ).reserveDocumentInsertions( 0 );
    }

    @Test
    public void updaterShouldReserveDocuments() throws Exception
    {
        // Given
        LuceneIndexWriter writer = mock( LuceneIndexWriter.class );
        IndexUpdater updater = newIndexUpdater( writer );

        // When
        updater.prepare( asList(
                add( 1, "value 1" ),
                add( 2, "value 2" ),
                add( 3, "value 3" ) ) );

        updater.prepare( asList(
                change( 0, "before 0", "after 0" ),
                add( 1, "value 1" ),
                add( 2, "vale 2" ),
                change( 3, "before 3", "after 3" ),
                remove( 4, "value 4" ) ) );

        updater.prepare( asList(
                change( 0, "before 0", "after 0" ),
                change( 1, "before 1", "after 1" ),
                remove( 2, "value 2" ),
                remove( 3, "value 3" ) ) );

        // Then
        InOrder order = inOrder( writer );
        order.verify( writer ).createSearcherManager();
        order.verify( writer ).reserveDocumentInsertions( 3 );
        order.verify( writer ).reserveDocumentInsertions( 4 );
        order.verify( writer ).reserveDocumentInsertions( 2 );
        verifyNoMoreInteractions( writer );
    }

    @Test
    public void reservationOfDocumentsShouldBeWithdrawnByCommit() throws Exception
    {
        // Given
        LuceneIndexWriter writer = mock( LuceneIndexWriter.class );
        IndexUpdater updater = newIndexUpdater( writer );

        // When
        PreparedIndexUpdates updates = updater.prepare( asList(
                add( 1, "value 1" ),
                add( 2, "value 2" ),
                change( 3, "before 3", "after 3" ),
                add( 3, "value 3" ),
                remove( 4, "value 4" ),
                remove( 5, "value 5" ) ) );

        updates.commit();

        // Then
        InOrder order = inOrder( writer );
        order.verify( writer ).createSearcherManager();
        order.verify( writer ).reserveDocumentInsertions( 4 );
        order.verify( writer, times( 2 ) ).addDocument( any( Document.class ) );
        order.verify( writer ).updateDocument( any( Term.class ), any( Document.class ) );
        order.verify( writer ).addDocument( any( Document.class ) );
        order.verify( writer, times( 2 ) ).deleteDocuments( any( Term.class ) );
        order.verify( writer ).removeReservationOfDocumentInsertions( 4 );
        verifyNoMoreInteractions( writer );
    }

    @Test
    public void reservationOfDocumentsShouldBeWithdrawnByRollback() throws Exception
    {
        // Given
        LuceneIndexWriter writer = mock( LuceneIndexWriter.class );
        IndexUpdater updater = newIndexUpdater( writer );

        // When
        PreparedIndexUpdates updates = updater.prepare( asList(
                change( 0, "before 0", "after 0" ),
                change( 1, "before 1", "after 1" ),
                add( 2, "value 2" ),
                remove( 3, "value 3" ),
                add( 4, "value 4" ) ) );

        updates.rollback();

        // Then
        InOrder order = inOrder( writer );
        order.verify( writer ).createSearcherManager();
        order.verify( writer ).reserveDocumentInsertions( 4 );
        order.verify( writer ).removeReservationOfDocumentInsertions( 4 );
        verifyNoMoreInteractions( writer );
    }

    @Test( expected = IllegalStateException.class )
    public void commitOfAlreadyCommittedUpdatesShouldThrow() throws Exception
    {
        // Given
        IndexUpdater updater = newIndexUpdater( mock( LuceneIndexWriter.class ) );
        PreparedIndexUpdates updates = updater.prepare( asList( add( 1, "value 1" ) ) );
        updates.commit();

        // When
        updates.commit();

        // Then
        // exception is thrown
    }

    private final long nodeId = 1, nodeId2 = 2;
    private final Object value = "value", value2 = 40;
    private final LuceneDocumentStructure documentLogic = new LuceneDocumentStructure();
    private final File dir = new File( "dir" );
    private LuceneIndexAccessor accessor;
    private DirectoryFactory.InMemoryDirectoryFactory dirFactory;
    
    @Before
    public void before() throws Exception
    {
        dirFactory = new DirectoryFactory.InMemoryDirectoryFactory();
        accessor = new NonUniqueLuceneIndexAccessor( documentLogic, reserving(), dirFactory, dir );
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
            throws IOException, IndexEntryConflictException
    {
        IndexUpdater updater = accessor.newUpdater( IndexUpdateMode.ONLINE );
        PreparedIndexUpdates preparedUpdates = updater.prepare( nodePropertyUpdates );
        preparedUpdates.commit();
    }

    private IndexUpdater newIndexUpdater( LuceneIndexWriter writer ) throws IOException
    {
        LuceneIndexWriterFactory indexWriterFactory = mock( LuceneIndexWriterFactory.class );
        when( indexWriterFactory.create( any( Directory.class ) ) ).thenReturn( writer );

        NonUniqueLuceneIndexAccessor indexAccessor =
                spy( new NonUniqueLuceneIndexAccessor( documentLogic, indexWriterFactory, dirFactory, dir ) );

        doNothing().when( indexAccessor ).refreshSearcherManager();

        return indexAccessor.newUpdater( IndexUpdateMode.ONLINE );
    }
}
