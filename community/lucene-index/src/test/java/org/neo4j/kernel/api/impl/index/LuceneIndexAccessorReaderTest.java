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

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.register.Registers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import static org.neo4j.helpers.CancellationRequest.NEVER_CANCELLED;

public class LuceneIndexAccessorReaderTest extends AbstractLuceneIndexAccessorReaderTest<LuceneIndexAccessorReader>
{
    @Before
    public void setup() throws IOException
    {
        this.accessor = new LuceneIndexAccessorReader( searcher, documentLogic, closeable, NEVER_CANCELLED, BUFFER_SIZE_LIMIT )
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
        // When
        when( fields.iterator() ).thenReturn( Arrays.asList( "id", "bool" ).iterator() );
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
        when( fields.iterator() ).thenReturn( Arrays.asList( "id", "string" ).iterator() );
        when( termsEnum.next() ).thenReturn( new BytesRef("aaa"), new BytesRef("ccc"), null );
        when( termsEnum.docFreq() ).thenReturn( 1, 2 );
        when( termsEnum.term() ).thenReturn(
                new BytesRef("aaa"),
                new BytesRef("ccc")
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
    public void shouldReturnNoValueTypesIfTheIndexIsEmpty() throws Exception
    {
        // Given
        when( fields.iterator() ).thenReturn( Collections.<String>emptyIterator() );

        // When
        final Set<Class> types = accessor.valueTypesInIndex();

        // Then
        assertTrue( types.isEmpty() );
    }

    @Test
    public void shouldReturnAllValueTypesContainedInTheIndex1() throws Exception
    {
        // Given
        when( fields.iterator() ).thenReturn( Arrays.asList( "array", "string", "array" ).iterator() );

        // When
        final Set<Class> types = accessor.valueTypesInIndex();

        // Then

        assertEquals( new HashSet<>( Arrays.asList( Array.class, String.class ) ), types );
    }

    @Test
    public void shouldReturnAllValueTypesContainedInTheIndex2() throws Exception
    {
        // Given
        when( fields.iterator() ).thenReturn( Arrays.asList( "array", "number", "string", "bool" ).iterator() );

        // When
        final Set<Class> types = accessor.valueTypesInIndex();

        // Then
        assertEquals( new HashSet<>( Arrays.asList( Array.class, Number.class, String.class, Boolean.class ) ), types );
    }

    @Test
    public void shouldWrapIOExceptionInRuntimeExceptionWhenAskingAllValueTypes() throws Exception
    {
        // Given
        final IOException ioex = new IOException();
        ((IndexReaderStub) reader).throwOnNextFieldsAccess( ioex );

        // When
        try
        {
            accessor.valueTypesInIndex();
            fail( "Should have thrown" );
        }
        catch ( RuntimeException ex )
        {
            // then
            assertSame( ioex, ex.getCause() );
        }
    }
}
