package org.neo4j.commons.iterator;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;
import org.neo4j.helpers.collection.FilteringIterator;

public class TestCommonIterators
{
    @Test
    public void testNoDuplicatesFilteringIterator()
    {
        List<Integer> ints = Arrays.asList( 1, 2, 2, 40, 100, 40, 101, 2, 3 );
        Iterator<Integer> iterator = FilteringIterator.noDuplicates( ints.iterator() );
        assertEquals( (Integer) 1, iterator.next() );
        assertEquals( (Integer) 2, iterator.next() );
        assertEquals( (Integer) 40, iterator.next() );
        assertEquals( (Integer) 100, iterator.next() );
        assertEquals( (Integer) 101, iterator.next() );
        assertEquals( (Integer) 3, iterator.next() );
    }
}
