/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.newapi;

import org.eclipse.collections.api.iterator.LongIterator;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RangeLongIteratorTest
{
    enum Mode
    {
        DIRECT,
        HEAP
    }

    @ParameterizedTest
    @EnumSource( Mode.class )
    void shouldHandleEmptyBuffer( Mode mode )
    {
        // given
        LongBuffer buffer = buffer( mode );

        // when
        RangeLongIterator iterator = new RangeLongIterator( buffer, 0, 0 );

        // then
        assertThat( iterator.hasNext(), equalTo( false ) );
        assertThrows( NoSuchElementException.class, iterator::next );
    }

    @ParameterizedTest
    @EnumSource( Mode.class )
    void shouldIterateOverSubsetOfData( Mode mode )
    {
        // given
        LongBuffer buffer = buffer( mode, 1L, 2L, 3L, 4L, 5L );

        // when
        RangeLongIterator iterator = new RangeLongIterator( buffer, 2, 4 );

        // then
        assertThat( iteratorAsList( iterator ), equalTo( asList( 3L, 4L ) ) );
    }

    @ParameterizedTest
    @EnumSource( Mode.class )
    void shouldNotBeAbleToCreateInvalidRanges( Mode mode )
    {
        // given
        LongBuffer buffer = buffer( mode, 1L, 2L, 3L, 4L, 5L );

        // expect
        assertThrows( IllegalArgumentException.class, () -> new RangeLongIterator( buffer, -1, 0 ) );
        assertThrows( IllegalArgumentException.class, () -> new RangeLongIterator( buffer, 0, -1 ) );
        assertThrows( IllegalArgumentException.class, () -> new RangeLongIterator( buffer, 2, 1 ) );
        assertThrows( IllegalArgumentException.class, () -> new RangeLongIterator( buffer, 10, 12 ) );
        assertThrows( IllegalArgumentException.class, () -> new RangeLongIterator( buffer, 0, 12 ) );
    }

    private List<Long> iteratorAsList( LongIterator iterator )
    {
        List<Long> list = new ArrayList<>();
        while ( iterator.hasNext() )
        {
            list.add( iterator.next() );
        }
        return list;
    }

    private LongBuffer buffer( Mode mode, long... values )
    {
        switch ( mode )
        {
        case DIRECT:
            ByteBuffer directBuffer = ByteBuffer.allocateDirect( Long.BYTES * values.length );
            for ( long value : values )
            {
                directBuffer.putLong( value );
            }
            directBuffer.flip();
            return directBuffer.asLongBuffer();
        case HEAP:
            return LongBuffer.wrap( values );
        default:
            throw new AssertionError( "oh noes!" );
        }
    }
}
