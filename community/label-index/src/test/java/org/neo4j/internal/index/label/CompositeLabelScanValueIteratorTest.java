/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.internal.index.label;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.collection.PrimitiveLongCollections;
import org.neo4j.collection.PrimitiveLongResourceIterator;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;
import static org.neo4j.collection.PrimitiveLongResourceCollections.emptyIterator;
import static org.neo4j.collection.PrimitiveLongResourceCollections.iterator;

@Execution( CONCURRENT )
class CompositeLabelScanValueIteratorTest
{
    @Test
    void mustHandleEmptyListOfIterators()
    {
        // given
        List<PrimitiveLongResourceIterator> iterators = emptyList();

        // when
        CompositeLabelScanValueIterator iterator = new CompositeLabelScanValueIterator( iterators, false );

        // then
        assertFalse( iterator.hasNext() );
        assertThrows( NoSuchElementException.class, iterator::next );
    }

    @Test
    void mustHandleEmptyIterator()
    {
        // given
        List<PrimitiveLongResourceIterator> iterators = singletonList( emptyIterator() );

        // when
        CompositeLabelScanValueIterator iterator = new CompositeLabelScanValueIterator( iterators, false );

        // then
        assertFalse( iterator.hasNext() );
    }

    @Test
    void mustHandleMultipleEmptyIterators()
    {
        // given
        List<PrimitiveLongResourceIterator> iterators =
                asMutableList( emptyIterator(), emptyIterator(), emptyIterator() );

        // when
        CompositeLabelScanValueIterator iterator = new CompositeLabelScanValueIterator( iterators, false );

        // then
        assertFalse( iterator.hasNext() );
    }

    /* ALL = FALSE */
    @Test
    void mustReportAllFromSingleIterator()
    {
        // given
        long[] expected = {0L, 1L, Long.MAX_VALUE};
        List<PrimitiveLongResourceIterator> iterators = Collections.singletonList( iterator( null, expected ) );

        // when
        CompositeLabelScanValueIterator iterator = new CompositeLabelScanValueIterator( iterators, false );

        // then
        assertArrayEquals( expected, PrimitiveLongCollections.asArray( iterator ) );
    }

    @Test
    void mustReportAllFromNonOverlappingMultipleIterators()
    {
        // given
        AtomicInteger closeCounter = new AtomicInteger();
        long[] firstIter  = {0L,     2L,     Long.MAX_VALUE};
        long[] secondIter = {    1L,     3L                };
        long[] expected   = {0L, 1L, 2L, 3L, Long.MAX_VALUE};
        List<PrimitiveLongResourceIterator> iterators = asMutableList(
                iterator( closeCounter::incrementAndGet, firstIter ),
                iterator( closeCounter::incrementAndGet, secondIter ) );

        // when
        CompositeLabelScanValueIterator iterator = new CompositeLabelScanValueIterator( iterators, false );

        // then
        assertArrayEquals( expected, PrimitiveLongCollections.asArray( iterator ) );

        // when
        iterator.close();

        // then
        assertEquals( 2, closeCounter.get(), "expected close count" );
    }

    @Test
    void mustReportUniqueValuesFromOverlappingIterators()
    {
        // given
        AtomicInteger closeCounter = new AtomicInteger();
        long[] firstIter  = {0L,     2L,     Long.MAX_VALUE};
        long[] secondIter = {    1L,     3L                };
        long[] thirdIter  = {0L,         3L                };
        long[] expected   = {0L, 1L, 2L, 3L, Long.MAX_VALUE};
        List<PrimitiveLongResourceIterator> iterators = asMutableList(
                iterator( closeCounter::incrementAndGet, firstIter ),
                iterator( closeCounter::incrementAndGet, secondIter ),
                iterator( closeCounter::incrementAndGet, thirdIter ) );

        // when
        CompositeLabelScanValueIterator iterator = new CompositeLabelScanValueIterator( iterators, false );

        // then
        assertArrayEquals( expected, PrimitiveLongCollections.asArray( iterator ) );

        // when
        iterator.close();

        // then
        assertEquals( 3, closeCounter.get(), "expected close count" );
    }

    @Test
    void mustReportUniqueValuesFromOverlappingIteratorsWithOneEmpty()
    {
        // given
        AtomicInteger closeCounter = new AtomicInteger();
        long[] firstIter  = {0L,     2L,     Long.MAX_VALUE};
        long[] secondIter = {    1L,     3L                };
        long[] thirdIter  = {0L,         3L                };
        long[] fourthIter = {/* Empty */                   };
        long[] expected   = {0L, 1L, 2L, 3L, Long.MAX_VALUE};
        List<PrimitiveLongResourceIterator> iterators = asMutableList(
                iterator( closeCounter::incrementAndGet, firstIter ),
                iterator( closeCounter::incrementAndGet, secondIter ),
                iterator( closeCounter::incrementAndGet, thirdIter ),
                iterator( closeCounter::incrementAndGet, fourthIter ) );

        // when
        CompositeLabelScanValueIterator iterator = new CompositeLabelScanValueIterator( iterators, false );

        // then
        assertArrayEquals( expected, PrimitiveLongCollections.asArray( iterator ) );

        // when
        iterator.close();

        // then
        assertEquals( 4, closeCounter.get(), "expected close count" );
    }

    /* ALL = TRUE */
    @Test
    void mustOnlyReportValuesReportedByAll()
    {
        // given
        AtomicInteger closeCounter = new AtomicInteger();
        long[] firstIter  = {0L,         Long.MAX_VALUE};
        long[] secondIter = {0L, 1L,     Long.MAX_VALUE};
        long[] thirdIter  = {0L, 1L, 2L, Long.MAX_VALUE};
        long[] expected   = {0L,         Long.MAX_VALUE};
        List<PrimitiveLongResourceIterator> iterators = asMutableList(
                iterator( closeCounter::incrementAndGet, firstIter ),
                iterator( closeCounter::incrementAndGet, secondIter ),
                iterator( closeCounter::incrementAndGet, thirdIter ) );

        // when
        CompositeLabelScanValueIterator iterator = new CompositeLabelScanValueIterator( iterators, true );

        // then
        assertArrayEquals( expected, PrimitiveLongCollections.asArray( iterator ) );

        // when
        iterator.close();

        // then
        assertEquals( 3, closeCounter.get(), "expected close count" );
    }

    @Test
    void mustOnlyReportValuesReportedByAllWithOneEmpty()
    {
        // given
        AtomicInteger closeCounter = new AtomicInteger();
        long[] firstIter  = {0L,         Long.MAX_VALUE};
        long[] secondIter = {0L, 1L,     Long.MAX_VALUE};
        long[] thirdIter  = {0L, 1L, 2L, Long.MAX_VALUE};
        long[] fourthIter = {/* Empty */               };
        long[] expected   = {                          };
        List<PrimitiveLongResourceIterator> iterators = asMutableList(
                iterator( closeCounter::incrementAndGet, firstIter ),
                iterator( closeCounter::incrementAndGet, secondIter ),
                iterator( closeCounter::incrementAndGet, thirdIter ),
                iterator( closeCounter::incrementAndGet, fourthIter ) );

        // when
        CompositeLabelScanValueIterator iterator = new CompositeLabelScanValueIterator( iterators, true );

        // then
        assertArrayEquals( expected, PrimitiveLongCollections.asArray( iterator ) );

        // when
        iterator.close();

        // then
        assertEquals( 4, closeCounter.get(), "expected close count" );
    }

    @SafeVarargs
    private static <T> List<T> asMutableList( T... objects )
    {
        return new ArrayList<>( Arrays.asList( objects ) );
    }
}
