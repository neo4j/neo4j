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

import org.apache.lucene.store.Directory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.InOrder;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.neo4j.kernel.api.impl.index.DirectoryFactory.InMemoryDirectoryFactory;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.Reservation;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith( Parameterized.class )
public class LuceneIndexAccessorReservationsTest
{
    private static final int PROPERTY_KEY = 42;
    private static final long[] LABELS = {42};

    @Parameter( 0 )
    public String name;
    @Parameter( 1 )
    public AccessorFactory accessorFactory;

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

    private IndexUpdater newIndexUpdater( ReservingLuceneIndexWriter writer ) throws IOException
    {
        return accessorFactory.create( writer ).newUpdater( IndexUpdateMode.ONLINE );
    }

    @Parameters( name = "{0}" )
    public static List<Object[]> accessorFactories()
    {
        return Arrays.asList(
                new Object[]{
                        UniqueLuceneIndexAccessor.class.getSimpleName(),
                        new AccessorFactory()
                        {
                            @Override
                            public IndexAccessor create( ReservingLuceneIndexWriter writer ) throws IOException
                            {
                                return new UniqueLuceneIndexAccessor( new LuceneDocumentStructure(),
                                        false, factoryOfOne( writer ), new InMemoryDirectoryFactory(), new
                                        File( "unique" ) );
                            }
                        }},
                new Object[]{
                        NonUniqueLuceneIndexAccessor.class.getSimpleName(),
                        new AccessorFactory()
                        {
                            @Override
                            public IndexAccessor create( ReservingLuceneIndexWriter writer ) throws IOException
                            {
                                return new NonUniqueLuceneIndexAccessor( new LuceneDocumentStructure(),
                                        false, factoryOfOne( writer ), new InMemoryDirectoryFactory(),
                                        new File( "non-unique" ), 100_000 );
                            }
                        }}
        );
    }

    @SuppressWarnings( "unchecked" )
    private static IndexWriterFactory<ReservingLuceneIndexWriter> factoryOfOne( ReservingLuceneIndexWriter writer )
            throws IOException
    {
        IndexWriterFactory<ReservingLuceneIndexWriter> factory = mock( IndexWriterFactory.class );
        doReturn( writer ).when( factory ).create( any( Directory.class ) );
        return factory;
    }

    private static NodePropertyUpdate add( long nodeId, Object value )
    {
        return NodePropertyUpdate.add( nodeId, PROPERTY_KEY, value, LABELS );
    }

    private static NodePropertyUpdate remove( long nodeId, Object value )
    {
        return NodePropertyUpdate.remove( nodeId, PROPERTY_KEY, value, LABELS );
    }

    private static NodePropertyUpdate change( long nodeId, Object valueBefore, Object valueAfter )
    {
        return NodePropertyUpdate.change( nodeId, PROPERTY_KEY, valueBefore, LABELS, valueAfter, LABELS );
    }

    private static interface AccessorFactory
    {
        IndexAccessor create( ReservingLuceneIndexWriter writer ) throws IOException;
    }
}
