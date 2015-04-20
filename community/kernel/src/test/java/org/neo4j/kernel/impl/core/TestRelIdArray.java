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
package org.neo4j.kernel.impl.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.kernel.impl.util.RelIdArray;
import org.neo4j.kernel.impl.util.RelIdArray.DirectionWrapper;
import org.neo4j.kernel.impl.util.RelIdIterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.kernel.impl.util.RelIdArray.DirectionWrapper.BOTH;
import static org.neo4j.kernel.impl.util.RelIdArray.DirectionWrapper.INCOMING;
import static org.neo4j.kernel.impl.util.RelIdArray.DirectionWrapper.OUTGOING;

// TODO Add some tests for loops, i.e. add with direction BOTH.
public class TestRelIdArray
{
    @Test
    public void testBasic() throws Exception
    {
        RelIdArray array = new RelIdArray( 0 );
        array.add( 1, OUTGOING );
        array.add( 2, OUTGOING );
        array.add( 3, INCOMING );

        // Iterate OUTGOING
        RelIdIterator itr = array.iterator( OUTGOING );
        assertTrue( itr.hasNext() );
        assertTrue( itr.hasNext() );
        assertEquals( 1L, itr.next() );
        assertTrue( itr.hasNext() );
        assertEquals( 2L, itr.next() );
        assertFalse( itr.hasNext() );
        assertFalse( itr.hasNext() );

        // Iterate INCOMING
        itr = array.iterator( INCOMING );
        assertTrue( itr.hasNext() );
        assertEquals( 3L, itr.next() );
        assertFalse( itr.hasNext() );

        // Iterate BOTH
        itr = array.iterator( BOTH );
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
        RelIdArray source = new RelIdArray( 0 );
        source.add( 1, OUTGOING );
        source.add( 2, OUTGOING );
        source.add( 3, INCOMING );
        source.add( 4, INCOMING );
        RelIdArray add = new RelIdArray( 0 );
        add.add( 5, OUTGOING );
        add.add( 6, OUTGOING );
        add.add( 7, OUTGOING );
        PrimitiveLongSet remove = Primitive.longSet();
        remove.add( 2L );
        remove.add( 6L );
        List<Long> allIds = asList( RelIdArray.from( source, add, remove ) );
        Collections.sort( allIds );
        assertEquals( Arrays.asList( 1L, 3L, 4L, 5L, 7L ), allIds );
    }

    @Test
    public void testDifferentBlocks() throws Exception
    {
        RelIdArray array = new RelIdArray( 0 );
        long justUnderIntMax = (long) Math.pow( 2, 32 )-3;
        array.add( justUnderIntMax, OUTGOING );
        array.add( justUnderIntMax+1, OUTGOING );
        long justOverIntMax = (long) Math.pow( 2, 32 )+3;
        array.add( justOverIntMax, OUTGOING );
        array.add( justOverIntMax+1, OUTGOING );
        long aBitOverIntMax = (long) Math.pow( 2, 33 );
        array.add( aBitOverIntMax, OUTGOING );
        array.add( aBitOverIntMax+1, OUTGOING );
        long verySmall = 1000;
        array.add( verySmall, OUTGOING );
        array.add( verySmall+1, OUTGOING );

        Collection<Long> allIds = new HashSet<Long>( asList( array ) );
        assertEquals( new HashSet<>( Arrays.asList(
                justUnderIntMax, justUnderIntMax+1,
                justOverIntMax, justOverIntMax+1,
                aBitOverIntMax, aBitOverIntMax+1,
                verySmall, verySmall+1 ) ), allIds );
    }

    @Test
    public void testAddDifferentBlocks() throws Exception
    {
        RelIdArray array1 = new RelIdArray( 0 );
        array1.add( 0, OUTGOING );
        array1.add( 1, OUTGOING );

        RelIdArray array2 = new RelIdArray( 0 );
        long justOverIntMax = (long) Math.pow( 2, 32 )+3;
        array2.add( justOverIntMax, OUTGOING );
        array2.add( justOverIntMax+1, OUTGOING );

        RelIdArray all = new RelIdArray( 0 );
        all.addAll( array1 );
        all.addAll( array2 );

        assertEquals( new HashSet<>( Arrays.asList(
                0L, 1L, justOverIntMax, justOverIntMax+1 ) ), new HashSet<>( asList( all ) ) );
    }

    @Test
    public void iterateThroughMultipleHighBitsSignaturesWhereIdsAreAdded() throws Exception
    {
        // GIVEN
        RelIdArray ids = new RelIdArray( 0 );
        ids.add( 0, OUTGOING );
        ids.add( 0x1FFFFFFFFL, OUTGOING );
        RelIdIterator iterator = ids.iterator( OUTGOING );

        // WHEN -- depleting the current iterator
        assertEquals( asSet( 0L, 0x1FFFFFFFFL ), deplete( iterator ) );

        // and adding one more id of the first "high bits" kind
        ids.add( 1, OUTGOING );
        iterator.updateSource( ids, OUTGOING );

        // THEN
        assertEquals( "Should see the added id after depleting two IdBlocks", asSet( 1L ), deplete( iterator ) );
    }

    @Test
    public void shouldAcceptAddsAfterAddAllInSameDirection() throws Exception
    {
        RelIdArray arrayTo = RelIdArray.empty( 1 );
        RelIdArray arrayFrom = RelIdArray.empty( 1 );
        arrayFrom.add( 1, DirectionWrapper.INCOMING );
        arrayTo.addAll( arrayFrom );
        arrayTo.add( 2, DirectionWrapper.INCOMING );
    }

    @Test
    public void shouldBeAbleToEvictLastIdInBlock() throws Exception
    {
        // GIVEN
        RelIdArray ids = new RelIdArray( 0 );
        ids.add( 0, OUTGOING );
        ids.add( 1, OUTGOING );
        ids.add( 2, OUTGOING );
        PrimitiveLongSet remove = Primitive.longSet( 1 );
        remove.add( 1 );

        // WHEN
        RelIdArray idsWithoutLast = RelIdArray.from( ids, null, remove );

        // THEN
        assertEquals( Arrays.asList( 0L, 2L ), asList( idsWithoutLast ) );
    }

    private Set<Long> deplete( RelIdIterator iterator )
    {
        HashSet<Long> set = new HashSet<>();
        while ( iterator.hasNext() )
        {
            set.add( iterator.next() );
        }
        return set;
    }

    private List<Long> asList( RelIdArray ids )
    {
        List<Long> result = new ArrayList<>();
        for ( RelIdIterator iterator = ids.iterator( DirectionWrapper.BOTH ); iterator.hasNext(); )
        {
            result.add( iterator.next() );
        }
        return result;
    }
}
