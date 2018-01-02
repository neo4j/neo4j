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

import java.io.IOException;

import org.apache.lucene.search.Query;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.register.Registers;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import static org.neo4j.helpers.CancellationRequest.NEVER_CANCELLED;

public class LuceneUniqueIndexAccessorReaderTest extends AbstractLuceneIndexAccessorReaderTest<LuceneUniqueIndexAccessorReader>
{
    @Before
    public void setup() throws IOException
    {
        this.accessor = new LuceneUniqueIndexAccessorReader( searcher, documentLogic, closeable, NEVER_CANCELLED )
        {
            @Override
            public PrimitiveLongIterator query( Query query )
            {
                return Primitive.iterator();
            }
        };

        when( searcher.getIndexReader() ).thenReturn( reader );
    }

    @Test
    public void shouldProvideTheIndexUniqueValuesForAnEmptyIndex() throws Exception
    {
        // Given
        when( reader.numDocs() ).thenReturn( 0 );

        // When
        final DoubleLongRegister output = Registers.newDoubleLongRegister();
        long indexSize = sampleAccessor( accessor, output );

        // Then
        assertEquals( 0, indexSize );
        assertEquals( 0, output.readFirst() );
        assertEquals( 0, output.readSecond() );
    }

    @Test
    public void shouldSkipTheNonNodeIdKeyEntriesWhenCalculatingIndexUniqueValues() throws Exception
    {
        // Given
        when( reader.numDocs() ).thenReturn( 2 );

        // When
        final DoubleLongRegister output = Registers.newDoubleLongRegister();
        long indexSize = sampleAccessor( accessor, output );

        // Then
        assertEquals( 2, indexSize );
        assertEquals( 2, output.readFirst() );
        assertEquals( 2, output.readSecond() );
    }

    private long sampleAccessor( LuceneIndexAccessorReader reader, DoubleLongRegister output )
            throws IndexNotFoundKernelException
    {
        return reader.sampleIndex( output );
    }
}
