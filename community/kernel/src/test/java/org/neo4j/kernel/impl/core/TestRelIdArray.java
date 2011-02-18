package org.neo4j.kernel.impl.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.neo4j.kernel.impl.util.RelIdArray;
import org.neo4j.kernel.impl.util.RelIdArray.RelIdIterator;

public class TestRelIdArray
{
    @Test
    public void testBasic() throws Exception
    {
        RelIdArray array = new RelIdArray();
        array.add( 1 );
        array.add( 2 );
        array.add( 3 );
        RelIdIterator itr = array.iterator();
        assertTrue( itr.hasNext() );
        assertTrue( itr.hasNext() );
        assertEquals( 1L, itr.next() );
        assertTrue( itr.hasNext() );
        assertEquals( 2L, itr.next() );
        assertTrue( itr.hasNext() );
        assertEquals( 3L, itr.next() );
        assertFalse( itr.hasNext() );
        assertFalse( itr.hasNext() );
    }
    
    @Test
    public void testWithAddRemove() throws Exception
    {
        RelIdArray source = new RelIdArray();
        source.add( 1 );
        source.add( 2 );
        source.add( 3 );
        source.add( 4 );
        RelIdArray add = new RelIdArray();
        add.add( 5 );
        add.add( 6 );
        add.add( 7 );
        RelIdArray remove = new RelIdArray();
        remove.add( 2 );
        remove.add( 6 );
        List<Long> allIds = asList( RelIdArray.from( source, add, remove ) );
        Collections.sort( allIds );
        assertEquals( Arrays.asList( 1L, 3L, 4L, 5L, 7L ), allIds );
    }
    
    private List<Long> asList( RelIdArray ids )
    {
        List<Long> result = new ArrayList<Long>();
        for ( RelIdIterator iterator = ids.iterator(); iterator.hasNext(); )
        {
            result.add( iterator.next() );
        }
        return result;
    }
}
