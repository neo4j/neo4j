package org.neo4j.kernel.impl.index.labelscan;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.emptyIterator;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.iterator;

public class CompositeLabelScanValueIteratorTest
{
    @Test
    public void mustHandleEmptyListOfIterators() throws Exception
    {
        // given
        List<PrimitiveLongIterator> iterators = emptyList();

        // when
        CompositeLabelScanValueIterator iterator = new CompositeLabelScanValueIterator( iterators, false );

        // then
        assertFalse( iterator.hasNext() );
        try
        {
            iterator.next();
            fail( "Expected iterator to throw" );
        }
        catch ( NoSuchElementException e )
        {
            // Good
        }
    }

    @Test
    public void mustHandleEmptyIterator() throws Exception
    {
        // given
        List<PrimitiveLongIterator> iterators = singletonList( emptyIterator() );

        // when
        CompositeLabelScanValueIterator iterator = new CompositeLabelScanValueIterator( iterators, false );

        // then
        assertFalse( iterator.hasNext() );
    }

    @Test
    public void mustHandleMultipleEmptyIterators() throws Exception
    {
        // given
        List<PrimitiveLongIterator> iterators = Arrays.asList( emptyIterator(), emptyIterator(), emptyIterator() );

        // when
        CompositeLabelScanValueIterator iterator = new CompositeLabelScanValueIterator( iterators, false );

        // then
        assertFalse( iterator.hasNext() );
    }

    /* ALL = FALSE */
    @Test
    public void mustReportAllFromSingleIterator() throws Exception
    {
        // given
        long[] expected = {0L, 1L, Long.MAX_VALUE};
        List<PrimitiveLongIterator> iterators = Arrays.asList( iterator( expected ) );

        // when
        CompositeLabelScanValueIterator iterator = new CompositeLabelScanValueIterator( iterators, false );

        // then
        assertArrayEquals( expected, PrimitiveLongCollections.asArray( iterator ) );
    }

    @Test
    public void mustReportAllFromNonOverlappingMultipleIterators() throws Exception
    {
        // given
        long[] firstIter  = {0L,     2L,     Long.MAX_VALUE};
        long[] secondIter = {    1L,     3L                };
        long[] expected   = {0L, 1L, 2L, 3L, Long.MAX_VALUE};
        List<PrimitiveLongIterator> iterators = Arrays.asList(
                iterator( firstIter ),
                iterator( secondIter ) );

        // when
        CompositeLabelScanValueIterator iterator = new CompositeLabelScanValueIterator( iterators, false );

        // then
        assertArrayEquals( expected, PrimitiveLongCollections.asArray( iterator ) );
    }

    @Test
    public void mustReportUniqueValuesFromOverlappingIterators() throws Exception
    {
        // given
        long[] firstIter  = {0L,     2L,     Long.MAX_VALUE};
        long[] secondIter = {    1L,     3L                };
        long[] thridIter  = {0L,         3L                };
        long[] expected   = {0L, 1L, 2L, 3L, Long.MAX_VALUE};
        List<PrimitiveLongIterator> iterators = Arrays.asList(
                iterator( firstIter ),
                iterator( secondIter ),
                iterator( thridIter ) );

        // when
        CompositeLabelScanValueIterator iterator = new CompositeLabelScanValueIterator( iterators, false );

        // then
        assertArrayEquals( expected, PrimitiveLongCollections.asArray( iterator ) );
    }

    @Test
    public void mustReportUniqueValuesFromOverlappingIteratorsWithOneEmpty() throws Exception
    {
        // given
        long[] firstIter  = {0L,     2L,     Long.MAX_VALUE};
        long[] secondIter = {    1L,     3L                };
        long[] thridIter  = {0L,         3L                };
        long[] fourthIter = {/* Empty */                   };
        long[] expected   = {0L, 1L, 2L, 3L, Long.MAX_VALUE};
        List<PrimitiveLongIterator> iterators = Arrays.asList(
                iterator( firstIter ),
                iterator( secondIter ),
                iterator( thridIter ),
                iterator( fourthIter ) );

        // when
        CompositeLabelScanValueIterator iterator = new CompositeLabelScanValueIterator( iterators, false );

        // then
        assertArrayEquals( expected, PrimitiveLongCollections.asArray( iterator ) );
    }

    /* ALL = TRUE */
    @Test
    public void mustOnlyReportValuesReportedByAll() throws Exception
    {
        // given
        long[] firstIter  = {0L,         Long.MAX_VALUE};
        long[] secondIter = {0L, 1L,     Long.MAX_VALUE};
        long[] thridIter  = {0L, 1L, 2L, Long.MAX_VALUE};
        long[] expected   = {0L,         Long.MAX_VALUE};
        List<PrimitiveLongIterator> iterators = Arrays.asList(
                iterator( firstIter ),
                iterator( secondIter ),
                iterator( thridIter ) );

        // when
        CompositeLabelScanValueIterator iterator = new CompositeLabelScanValueIterator( iterators, true );

        // then
        assertArrayEquals( expected, PrimitiveLongCollections.asArray( iterator ) );
    }

    @Test
    public void mustOnlyReportValuesReportedByAllWithOneEmpty() throws Exception
    {
        // given
        long[] firstIter  = {0L,         Long.MAX_VALUE};
        long[] secondIter = {0L, 1L,     Long.MAX_VALUE};
        long[] thridIter  = {0L, 1L, 2L, Long.MAX_VALUE};
        long[] fourthIter = {/* Empty */               };
        long[] expected   = {                          };
        List<PrimitiveLongIterator> iterators = Arrays.asList(
                iterator( firstIter ),
                iterator( secondIter ),
                iterator( thridIter ),
                iterator( fourthIter ));

        // when
        CompositeLabelScanValueIterator iterator = new CompositeLabelScanValueIterator( iterators, true );

        // then
        assertArrayEquals( expected, PrimitiveLongCollections.asArray( iterator ) );
    }
}