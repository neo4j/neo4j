/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.counts;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory;
import org.neo4j.kernel.impl.store.counts.keys.IndexSampleKey;
import org.neo4j.kernel.impl.store.counts.keys.IndexStatisticsKey;
import org.neo4j.kernel.impl.store.counts.keys.NodeKey;
import org.neo4j.kernel.impl.store.counts.keys.RelationshipKey;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class InMemoryCountsStoreTest
{
    @Test
    public void getExpectedValue()
    {
        //GIVEN
        InMemoryCountsStore countStore = new InMemoryCountsStore();
        Map<CountsKey,long[]> update = new HashMap<>();
        NodeKey key = CountsKeyFactory.nodeKey( 1 );
        update.put( key, new long[]{1} );

        //WHEN
        countStore.updateAll( 1, update );

        //THEN
        assertEquals( countStore.get( key )[0], 1 );
    }

    @Test
    public void neverSetKeyReturnsNull()
    {
        //GIVEN
        InMemoryCountsStore countStore = new InMemoryCountsStore();
        Map<CountsKey,long[]> update = new HashMap<>();
        NodeKey key = CountsKeyFactory.nodeKey( 1 );
        update.put( key, new long[]{1} );

        //WHEN
        countStore.updateAll( 1, update );

        //THEN
        Assert.assertNull( countStore.get( CountsKeyFactory.relationshipKey( 1, 1, 1 ) ) );
    }

    @Test( expected = NullPointerException.class )
    public void getNullKeyResultsInNPE()
    {
        //GIVEN
        InMemoryCountsStore countStore = new InMemoryCountsStore();
        Map<CountsKey,long[]> update = new HashMap<>();
        NodeKey key = CountsKeyFactory.nodeKey( 1 );
        update.put( key, new long[]{1} );

        //WHEN
        countStore.updateAll( 1, update );

        //THEN throws
        countStore.get( null );
    }


    @Test
    public void emptyUpdate()
    {
        //GIVEN
        InMemoryCountsStore countStore = new InMemoryCountsStore();
        Map<CountsKey,long[]> update = new HashMap<>();

        //WHEN
        countStore.updateAll( 1, update );

        //THEN
        CountsSnapshot countsSnapshot = countStore.snapshot( 1 );
        assertEquals( countsSnapshot.getTxId(), 1 );
        assertEquals( countsSnapshot.getMap().size(), 0 );
    }

    @Test
    public void validSnapshot()
    {
        //GIVEN
        InMemoryCountsStore countStore = new InMemoryCountsStore();
        Map<CountsKey,long[]> update = new HashMap<>();
        NodeKey key = CountsKeyFactory.nodeKey( 1 );
        update.put( key, new long[]{1} );

        //WHEN
        countStore.updateAll( 1, update );

        //THEN
        CountsSnapshot countsSnapshot = countStore.snapshot( 1 );
        assertEquals( countsSnapshot.getTxId(), 1 );
        assertEquals( countsSnapshot.getMap().size(), 1 );
        assertEquals( countsSnapshot.getMap().get( key )[0], 1 );
    }

    @Test
    public void restoreFromSnapshot()
    {
        //GIVEN
        InMemoryCountsStore countStore = new InMemoryCountsStore();
        Map<CountsKey,long[]> update = new HashMap<>();
        NodeKey keyA = CountsKeyFactory.nodeKey( 1 );
        NodeKey keyB = CountsKeyFactory.nodeKey( 2 );
        NodeKey keyC = CountsKeyFactory.nodeKey( 3 );

        update.put( keyA, new long[]{1} );
        countStore.updateAll( 1, update );
        update.clear();

        update.put( keyB, new long[]{1} );
        countStore.updateAll( 2, update );
        update.clear();

        update.put( keyC, new long[]{1} );
        countStore.updateAll( 3, update );

        //WHEN
        CountsSnapshot countsSnapshot = countStore.snapshot( 3 );
        long beforeTxId = countsSnapshot.getTxId();
        assertEquals( 3, beforeTxId );
        countStore = new InMemoryCountsStore( countsSnapshot );

        //THEN
        CountsSnapshot secondCountsSnapshot = countStore.snapshot( 3 );
        assertEquals( 3, secondCountsSnapshot.getTxId() );
        update.put( keyC, new long[]{1} );
        countStore.updateAll( 4, update );
        CountsSnapshot thirdCountsSnapshot = countStore.snapshot( 4 );
        assertEquals( 4, thirdCountsSnapshot.getTxId() );
    }

    @Test
    public void testForEach() throws Exception
    {
        InMemoryCountsStore countStore = new InMemoryCountsStore();
        Map<CountsKey,long[]> update = new HashMap<>();
        NodeKey nodeKey = CountsKeyFactory.nodeKey( 1 );
        update.put( nodeKey, new long[]{1} );
        countStore.updateAll( 1, update );

        countStore.forEach( new BiConsumer<CountsKey,long[]>()
        {
            @Override
            public void accept( CountsKey countsKey, long[] longs )
            {
                assertEquals( nodeKey, countsKey );
                assertEquals( 1, longs[0] );
            }
        } );
    }

    @Test
    public void testEmptyForEach() throws Exception
    {
        InMemoryCountsStore countStore = new InMemoryCountsStore();

        countStore.forEach( new BiConsumer<CountsKey,long[]>()
        {
            @Override
            public void accept( CountsKey countsKey, long[] longs )
            {
                fail();
            }
        } );
    }

    @Test
    public void testUpdate()
    {
        //GIVEN
        InMemoryCountsStore countStore = new InMemoryCountsStore();
        NodeKey nodeKey = CountsKeyFactory.nodeKey( 1 );
        RelationshipKey relKey = CountsKeyFactory.relationshipKey( 1, 1, 1 );
        IndexSampleKey indexSampleKey = CountsKeyFactory.indexSampleKey( 1, 1 );
        IndexStatisticsKey indexStatisticsKey = CountsKeyFactory.indexStatisticsKey( 1, 1 );

        //WHEN
        countStore.update( nodeKey, new long[]{1} );
        //THEN
        assertEquals( countStore.get( nodeKey )[0], 1 );

        //WHEN
        countStore.update( nodeKey, new long[]{1} );
        //THEN
        assertEquals( countStore.get( nodeKey )[0], 2 );


        //WHEN
        countStore.update( relKey, new long[]{1} );
        //THEN
        assertEquals( countStore.get( relKey )[0], 1 );

        //WHEN
        countStore.update( relKey, new long[]{1} );
        //THEN
        assertEquals( countStore.get( relKey )[0], 2 );


        //WHEN
        countStore.update( indexSampleKey, new long[]{1, 1} );
        //THEN
        assertEquals( countStore.get( indexSampleKey )[0], 1 );
        assertEquals( countStore.get( indexSampleKey )[1], 1 );

        //WHEN
        countStore.update( indexSampleKey, new long[]{1, 1} );
        //THEN
        assertEquals( countStore.get( indexSampleKey )[0], 2 );
        assertEquals( countStore.get( indexSampleKey )[1], 2 );


        //WHEN
        countStore.update( indexStatisticsKey, new long[]{1, 1} );
        //THEN
        assertEquals( countStore.get( indexStatisticsKey )[0], 1 );
        assertEquals( countStore.get( indexStatisticsKey )[1], 1 );

        //WHEN
        countStore.update( indexStatisticsKey, new long[]{1, 1} );
        //THEN
        assertEquals( countStore.get( indexStatisticsKey )[0], 2 );
        assertEquals( countStore.get( indexStatisticsKey )[1], 2 );
    }

    @Test
    public void testReplace()
    {
        //GIVEN
        InMemoryCountsStore countStore = new InMemoryCountsStore();
        NodeKey nodeKey = CountsKeyFactory.nodeKey( 1 );
        RelationshipKey relKey = CountsKeyFactory.relationshipKey( 1, 1, 1 );
        IndexSampleKey indexSampleKey = CountsKeyFactory.indexSampleKey( 1, 1 );
        IndexStatisticsKey indexStatisticsKey = CountsKeyFactory.indexStatisticsKey( 1, 1 );

        //WHEN
        countStore.update( nodeKey, new long[]{42} );
        countStore.replace( nodeKey, new long[]{13} );
        //THEN
        assertEquals( countStore.get( nodeKey )[0], 13 );

        //WHEN
        countStore.update( relKey, new long[]{42} );
        countStore.replace( relKey, new long[]{13} );
        //THEN
        assertEquals( countStore.get( relKey )[0], 13 );

        //WHEN
        countStore.update( indexSampleKey, new long[]{42, 24} );
        countStore.replace( indexSampleKey, new long[]{13, 14} );
        //THEN
        assertEquals( countStore.get( indexSampleKey )[0], 13 );
        assertEquals( countStore.get( indexSampleKey )[1], 14 );

        //WHEN
        countStore.update( indexStatisticsKey, new long[]{42, 24} );
        countStore.replace( indexStatisticsKey, new long[]{13, 14} );
        //THEN
        assertEquals( countStore.get( indexStatisticsKey )[0], 13 );
        assertEquals( countStore.get( indexStatisticsKey )[1], 14 );
    }

    @Test
    public void testInterleavedUpdateAllAndUpdate()
    {
        //GIVEN
        InMemoryCountsStore countStore = new InMemoryCountsStore();
        NodeKey nodeKey = CountsKeyFactory.nodeKey( 1 );
        RelationshipKey relKey = CountsKeyFactory.relationshipKey( 1, 1, 1 );
        IndexSampleKey indexSampleKey = CountsKeyFactory.indexSampleKey( 1, 1 );
        IndexStatisticsKey indexStatisticsKey = CountsKeyFactory.indexStatisticsKey( 1, 1 );

        //WHEN
        countStore.updateAll( 1, new HashMap<CountsKey,long[]>()
        {{
            put( nodeKey, new long[]{42} );
            put( relKey, new long[]{89} );
            put( indexSampleKey, new long[]{55, 44} );
            put( indexStatisticsKey, new long[]{33, 22} );
        }} );

        countStore.update( nodeKey, new long[]{10} );
        countStore.update( relKey, new long[]{10} );
        countStore.update( indexSampleKey, new long[]{10, 10} );
        countStore.update( indexStatisticsKey, new long[]{10, 10} );

        //THEN
        assertEquals( countStore.get( nodeKey )[0], 52 );
        assertEquals( countStore.get( relKey )[0], 99 );
        assertEquals( countStore.get( indexSampleKey )[0], 65 );
        assertEquals( countStore.get( indexSampleKey )[1], 54 );
        assertEquals( countStore.get( indexStatisticsKey )[0], 43 );
        assertEquals( countStore.get( indexStatisticsKey )[1], 32 );

    }

    @Test
    public void testInterleavedUpdateAllAndReplace()
    {
        //GIVEN
        InMemoryCountsStore countStore = new InMemoryCountsStore();
        NodeKey nodeKey = CountsKeyFactory.nodeKey( 1 );
        RelationshipKey relKey = CountsKeyFactory.relationshipKey( 1, 1, 1 );
        IndexSampleKey indexSampleKey = CountsKeyFactory.indexSampleKey( 1, 1 );
        IndexStatisticsKey indexStatisticsKey = CountsKeyFactory.indexStatisticsKey( 1, 1 );

        //WHEN
        countStore.updateAll( 1, new HashMap<CountsKey,long[]>()
        {{
            put( nodeKey, new long[]{42} );
            put( relKey, new long[]{89} );
            put( indexSampleKey, new long[]{55, 44} );
            put( indexStatisticsKey, new long[]{33, 22} );
        }} );

        countStore.replace( nodeKey, new long[]{10} );
        countStore.replace( relKey, new long[]{10} );
        countStore.replace( indexSampleKey, new long[]{10, 10} );
        countStore.replace( indexStatisticsKey, new long[]{10, 10} );

        //THEN
        assertEquals( countStore.get( nodeKey )[0], 10 );
        assertEquals( countStore.get( relKey )[0], 10 );
        assertEquals( countStore.get( indexSampleKey )[0], 10 );
        assertEquals( countStore.get( indexSampleKey )[1], 10 );
        assertEquals( countStore.get( indexStatisticsKey )[0], 10 );
        assertEquals( countStore.get( indexStatisticsKey )[1], 10 );

    }
}
