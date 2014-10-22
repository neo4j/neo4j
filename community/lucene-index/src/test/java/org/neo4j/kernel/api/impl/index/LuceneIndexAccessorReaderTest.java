/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.api.impl.index.LuceneDocumentStructure.NODE_ID_KEY;

import java.io.Closeable;
import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.search.IndexSearcher;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.kernel.api.index.ValueSampler;
import org.neo4j.kernel.impl.api.index.sampling.BoundedIndexSampler;
import org.neo4j.kernel.impl.api.index.sampling.UniqueIndexSizeSampler;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.register.Registers;

public class LuceneIndexAccessorReaderTest
{
    private final Closeable closeable = mock( Closeable.class );
    private final LuceneDocumentStructure documentLogic = mock( LuceneDocumentStructure.class );
    private final IndexSearcher searcher = mock( IndexSearcher.class );
    private final IndexReader reader = mock( IndexReader.class );
    private final TermEnum terms = mock( TermEnum.class );

    @Before
    public void setup() throws IOException
    {
        when( searcher.getIndexReader() ).thenReturn( reader );
        when( reader.terms() ).thenReturn( terms );
    }

    @Test
    public void shouldProvideTheIndexUniqueValuesForAnEmptyIndex()
    {
        // Given
        final LuceneIndexAccessorReader accessor = new LuceneIndexAccessorReader( searcher, documentLogic, closeable );

        // When
        final DoubleLongRegister output = Registers.newDoubleLongRegister();
        long indexSize = sampleAccessor( accessor, output );

        // Then
        assertEquals( 0, indexSize );
        assertEquals( 0, output.readFirst() );
        assertEquals( 0, output.readSecond() );
    }

    @Test
    public void shouldProvideTheIndexUniqueValuesForAnIndexWithDuplicates() throws IOException
    {
        // Given
        when( terms.next() ).thenReturn( true, true, true, false );
        when( terms.term() ).thenReturn(
                new Term( "string", "aaa" ),
                new Term( "string", "ccc" ),
                new Term( "string", "ccc" )
        );

        final LuceneIndexAccessorReader accessor = new LuceneIndexAccessorReader( searcher, documentLogic, closeable );

        // When
        final DoubleLongRegister output = Registers.newDoubleLongRegister();
        long indexSize = sampleAccessor( accessor, output );

        // Then
        assertEquals( 3, indexSize );
        assertEquals( 2, output.readFirst() );
        assertEquals( 3, output.readSecond() );
    }


    @Test
    public void shouldSkipTheNonNodeIdKeyEntriesWhenCalculatingIndexUniqueValues() throws IOException
    {
        // Given
        when( terms.next() ).thenReturn( true, true, false );
        when( terms.term() ).thenReturn(
                new Term( NODE_ID_KEY, "aaa" ), // <- this should be ignored
                new Term( "string", "bbb" )
        );

        final LuceneIndexAccessorReader accessor = new LuceneIndexAccessorReader( searcher, documentLogic, closeable );

        // When

        final DoubleLongRegister output = Registers.newDoubleLongRegister();
        long indexSize = sampleAccessor( accessor, output );

        // Then
        assertEquals( 1, indexSize );
        assertEquals( 1, output.readFirst() );
        assertEquals( 1, output.readSecond() );
    }

    @Test
    public void shouldWrapAnIOExceptionIntoARuntimeExceptionWhenCalculatingIndexUniqueValues() throws IOException
    {
        // Given
        final IOException ioex = new IOException();
        when( terms.next() ).thenThrow( ioex );
        final LuceneIndexAccessorReader accessor = new LuceneIndexAccessorReader( searcher, documentLogic, closeable );

        // When
        try
        {
            sampleAccessor( accessor, Registers.newDoubleLongRegister() );
            fail( "should have thrown" );
        }
        catch ( RuntimeException ex )
        {
            // Then
            assertSame( ioex, ex.getCause() );
        }
    }

    private long sampleAccessor( LuceneIndexAccessorReader accessor, DoubleLongRegister output )
    {
        ValueSampler sampler = new BoundedIndexSampler( 10_000 );
        accessor.sampleIndex( sampler );
        return sampler.result( output );
    }
}
