package org.neo4j.graphalgo.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;
import org.neo4j.graphalgo.util.PriorityMap.Entry;

public class TestPriorityMap
{
    @Test
    public void testIt()
    {
        PriorityMap<Integer, Integer, Double> map =
                PriorityMap.withSelfKeyNaturalOrder();
//        map.put( 0, 5d );
//        map.put( 1, 4d );
//        map.put( 1, 4d );
//        map.put( 1, 3d );
//        assertEntry( map.pop(), 1, 3d );
//        assertEntry( map.pop(), 0, 5d );
//        assertNull( map.pop() );
        
        int start = 0, a = 1, b = 2, c = 3, d = 4, e = 6, f = 7, y = 8, x = 9;
        map.put( start, 0d );
        map.put( a, 1d );
        // get start
        // get a
        map.put( x, 10d );
        map.put( b, 2d );
        // get b
        map.put( x, 9d );
        map.put( c, 3d );
        // get c
        map.put( x, 8d );
        map.put( x, 6d );
        map.put( d, 4d );
        // get d
        map.put( x, 7d );
        map.put( e, 5d );
        // get e
        map.put( x, 6d );
        map.put( f, 7d );
        // get x
        map.put( y, 8d );
        // get x
//        map.put( 
    }

    private void assertEntry( Entry<Integer, Double> entry, Integer entity,
            Double priority )
    {
        assertNotNull( entry );
        assertEquals( entity, entry.getEntity() );
        assertEquals( priority, entry.getPriority() );
    }
}
