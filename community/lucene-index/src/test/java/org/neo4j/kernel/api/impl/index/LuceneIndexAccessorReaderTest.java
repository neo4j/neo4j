/*
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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.search.IndexSearcher;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.register.Registers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.CancellationRequest.NEVER_CANCELLED;
import static org.neo4j.kernel.api.impl.index.LuceneDocumentStructure.NODE_ID_KEY;

public class LuceneIndexAccessorReaderTest
{
    private static final int BUFFER_SIZE_LIMIT = 100_000;

    private final Closeable closeable = mock( Closeable.class );
    private final LuceneDocumentStructure documentLogic = mock( LuceneDocumentStructure.class );
    private final IndexSearcher searcher = mock( IndexSearcher.class );
    private final IndexReader reader = mock( IndexReader.class );
    private final TermEnum terms = mock( TermEnum.class );
    private final LuceneIndexAccessorReader accessor =
            new LuceneIndexAccessorReader( searcher, documentLogic, closeable, NEVER_CANCELLED, BUFFER_SIZE_LIMIT );

    @Before
    public void setup() throws IOException
    {
        when( searcher.getIndexReader() ).thenReturn( reader );
        when( reader.terms() ).thenReturn( terms );
    }

    @Test
    public void shouldProvideTheIndexUniqueValuesForAnEmptyIndex() throws Exception
    {
        // When
        final DoubleLongRegister output = Registers.newDoubleLongRegister();
        long indexSize = accessor.sampleIndex( output );

        // Then
        assertEquals( 0, indexSize );
        assertEquals( 0, output.readFirst() );
        assertEquals( 0, output.readSecond() );
    }

    @Test
    public void shouldProvideTheIndexUniqueValuesForAnIndexWithDuplicates() throws Exception
    {
        // Given
        when( terms.next() ).thenReturn( true, true, true, false );
        when( terms.term() ).thenReturn(
                new Term( "string", "aaa" ),
                new Term( "string", "ccc" ),
                new Term( "string", "ccc" )
        );

        // When
        final DoubleLongRegister output = Registers.newDoubleLongRegister();
        long indexSize = accessor.sampleIndex( output );

        // Then
        assertEquals( 3, indexSize );
        assertEquals( 2, output.readFirst() );
        assertEquals( 3, output.readSecond() );
    }


    @Test
    public void shouldSkipTheNonNodeIdKeyEntriesWhenCalculatingIndexUniqueValues() throws Exception
    {
        // Given
        when( terms.next() ).thenReturn( true, true, false );
        when( terms.term() ).thenReturn(
                new Term( NODE_ID_KEY, "aaa" ), // <- this should be ignored
                new Term( "string", "bbb" )
        );

        // When

        final DoubleLongRegister output = Registers.newDoubleLongRegister();
        long indexSize = accessor.sampleIndex( output );

        // Then
        assertEquals( 1, indexSize );
        assertEquals( 1, output.readFirst() );
        assertEquals( 1, output.readSecond() );
    }

    @Test
    public void shouldWrapAnIOExceptionIntoARuntimeExceptionWhenCalculatingIndexUniqueValues() throws Exception
    {
        // Given
        final IOException ioex = new IOException();
        when( terms.next() ).thenThrow( ioex );

        // When
        try
        {
            accessor.sampleIndex( Registers.newDoubleLongRegister() );
            fail( "should have thrown" );
        }
        catch ( RuntimeException ex )
        {
            // Then
            assertSame( ioex, ex.getCause() );
        }
    }

    @Test
    public void shouldReturnNoValueTypesIfTheIndexIsEmpty() throws Exception
    {
        // Given
        when( terms.next() ).thenReturn( false );

        // When
        final Set<Class> types = accessor.valueTypesInIndex();

        // Then
        assertTrue( types.isEmpty() );
    }

    @Test
    public void shouldReturnAllValueTypesContainedInTheIndex1() throws Exception
    {
        // Given
        when( terms.next() ).thenReturn( true, true, true, false );
        when( terms.term() ).thenReturn( new Term( "array" ), new Term( "string" ), new Term( "array" ) );

        // When
        final Set<Class> types = accessor.valueTypesInIndex();

        // Then

        assertEquals( new HashSet<Class>( Arrays.asList( Array.class, String.class ) ), types );
    }

    @Test
    public void shouldReturnAllValueTypesContainedInTheIndex2() throws Exception
    {
        // Given
        when( terms.next() ).thenReturn( true, true, true, true, false );
        when( terms.term() )
                .thenReturn( new Term( "array" ), new Term( "number" ), new Term( "string" ), new Term( "bool" ) );

        // When
        final Set<Class> types = accessor.valueTypesInIndex();

        // Then
        assertEquals( new HashSet<Class>( Arrays.asList( Array.class, Number.class, String.class, Boolean.class ) ), types );
    }

    @Test
    public void shouldWrapIOExceptionInRuntimeExceptionWhenAskingAllValueTypes() throws Exception
    {
        // Given
        final IOException ioex = new IOException();
        when( terms.next() ).thenThrow( ioex );

        // When
        try
        {
            accessor.valueTypesInIndex();
            fail("Should have thrown");
        }
        catch ( RuntimeException ex )
        {
            // then
            assertSame( ioex, ex.getCause() );
        }
    }
}
