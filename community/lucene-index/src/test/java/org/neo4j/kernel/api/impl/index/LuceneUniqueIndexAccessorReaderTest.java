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

import java.io.Closeable;
import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.search.IndexSearcher;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.register.Registers;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.kernel.api.impl.index.LuceneDocumentStructure.NODE_ID_KEY;

public class LuceneUniqueIndexAccessorReaderTest
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
        final LuceneUniqueIndexAccessorReader accessor = new LuceneUniqueIndexAccessorReader( searcher, documentLogic, closeable );

        // When
        DoubleLongRegister output = sampleAccessor( accessor, 1, 0 );

        // Then
        assertEquals( 0, output.readFirst() );
        assertEquals( 0, output.readSecond() );
    }

    @Test
    public void shouldSkipTheNonNodeIdKeyEntriesWhenCalculatingIndexUniqueValues() throws IOException
    {
        // Given
        when( terms.next() ).thenReturn( true, true, false );
        when( terms.term() ).thenReturn(
                new Term( NODE_ID_KEY, "aaa" ), // <- this should be ignored
                new Term( "string", "bbb" ),
                new Term( "string", "ccc" )
        );

        final LuceneUniqueIndexAccessorReader accessor = new LuceneUniqueIndexAccessorReader( searcher, documentLogic, closeable );

        // When
        DoubleLongRegister output = sampleAccessor( accessor, 3, 2 );

        // Then
        assertEquals( 0, output.readFirst() );
        assertEquals( 0, output.readSecond() );
    }

    private DoubleLongRegister sampleAccessor( LuceneIndexAccessorReader accessor, int sampleSize, int indexSize )
    {
        final DoubleLongRegister output = Registers.newDoubleLongRegister();
        accessor.sampleIndex( new SkipOracleSampler( SkipOracle.Factory.FULL_SCAN_SKIP_ORACLE ), output );
        return output;
    }
}
