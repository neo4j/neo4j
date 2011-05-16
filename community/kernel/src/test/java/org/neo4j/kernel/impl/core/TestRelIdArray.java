/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.kernel.impl.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.impl.util.RelIdArray;
import org.neo4j.kernel.impl.util.RelIdArray.DirectionWrapper;
import org.neo4j.kernel.impl.util.RelIdArray.RelIdIterator;

public class TestRelIdArray
{
    @Test
    public void testBasic() throws Exception
    {
        RelIdArray array = new RelIdArray();
        array.add( 1, Direction.OUTGOING );
        array.add( 2, Direction.OUTGOING );
        array.add( 3, Direction.INCOMING );
        
        // Iterate OUTGOING
        RelIdIterator itr = array.iterator( DirectionWrapper.OUTGOING );
        assertTrue( itr.hasNext() );
        assertTrue( itr.hasNext() );
        assertEquals( 1L, itr.next() );
        assertTrue( itr.hasNext() );
        assertEquals( 2L, itr.next() );
        assertFalse( itr.hasNext() );
        assertFalse( itr.hasNext() );
        
        // Iterate INCOMING
        itr = array.iterator( DirectionWrapper.INCOMING );
        assertTrue( itr.hasNext() );
        assertEquals( 3L, itr.next() );
        assertFalse( itr.hasNext() );
        
        // Iterate BOTH
        itr = array.iterator( DirectionWrapper.BOTH );
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
        source.add( 1, Direction.OUTGOING );
        source.add( 2, Direction.OUTGOING );
        source.add( 3, Direction.INCOMING );
        source.add( 4, Direction.INCOMING );
        RelIdArray add = new RelIdArray();
        add.add( 5, Direction.OUTGOING );
        add.add( 6, Direction.OUTGOING );
        add.add( 7, Direction.OUTGOING );
        RelIdArray remove = new RelIdArray();
        remove.add( 2, Direction.OUTGOING );
        remove.add( 6, Direction.OUTGOING );
        List<Long> allIds = asList( RelIdArray.from( source, add, remove ) );
        Collections.sort( allIds );
        assertEquals( Arrays.asList( 1L, 3L, 4L, 5L, 7L ), allIds );
    }
    
    @Test
    public void testDifferentBlocks() throws Exception
    {
        RelIdArray array = new RelIdArray();
        long justUnderIntMax = (long) Math.pow( 2, 32 )-3;
        array.add( justUnderIntMax, Direction.OUTGOING );
        array.add( justUnderIntMax+1, Direction.OUTGOING );
        long justOverIntMax = (long) Math.pow( 2, 32 )+3;
        array.add( justOverIntMax, Direction.OUTGOING );
        array.add( justOverIntMax+1, Direction.OUTGOING );
        long aBitOverIntMax = (long) Math.pow( 2, 33 );
        array.add( aBitOverIntMax, Direction.OUTGOING );
        array.add( aBitOverIntMax+1, Direction.OUTGOING );
        long verySmall = 1000;
        array.add( verySmall, Direction.OUTGOING );
        array.add( verySmall+1, Direction.OUTGOING );
        
        Collection<Long> allIds = new HashSet<Long>( asList( array ) );
        assertEquals( new HashSet<Long>( Arrays.asList(
                justUnderIntMax, justUnderIntMax+1,
                justOverIntMax, justOverIntMax+1,
                aBitOverIntMax, aBitOverIntMax+1,
                verySmall, verySmall+1 ) ), allIds );
    }
    
    @Test
    public void testAddDifferentBlocks() throws Exception
    {
        RelIdArray array1 = new RelIdArray();
        array1.add( 0, Direction.OUTGOING );
        array1.add( 1, Direction.OUTGOING );
        
        RelIdArray array2 = new RelIdArray();
        long justOverIntMax = (long) Math.pow( 2, 32 )+3;
        array2.add( justOverIntMax, Direction.OUTGOING );
        array2.add( justOverIntMax+1, Direction.OUTGOING );
        
        RelIdArray all = new RelIdArray();
        all.addAll( array1 );
        all.addAll( array2 );
        
        assertEquals( new HashSet<Long>( Arrays.asList(
                0L, 1L, justOverIntMax, justOverIntMax+1 ) ), new HashSet<Long>( asList( all ) ) );
    }
    
    private List<Long> asList( RelIdArray ids )
    {
        List<Long> result = new ArrayList<Long>();
        for ( RelIdIterator iterator = ids.iterator( DirectionWrapper.BOTH ); iterator.hasNext(); )
        {
            result.add( iterator.next() );
        }
        return result;
    }
}
