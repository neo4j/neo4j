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
package org.neo4j.kernel.impl.store.countStore;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory;
import org.neo4j.kernel.impl.store.counts.keys.NodeKey;

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
        Assert.assertEquals( countStore.get( key )[0], 1 );
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
        Assert.assertEquals( countsSnapshot.getTxId(), 1 );
        Assert.assertEquals( countsSnapshot.getMap().size(), 0 );
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
        Assert.assertEquals( countsSnapshot.getTxId(), 1 );
        Assert.assertEquals( countsSnapshot.getMap().size(), 1 );
        Assert.assertEquals( countsSnapshot.getMap().get( key )[0], 1 );
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
        Assert.assertEquals( 3, beforeTxId );
        countStore = new InMemoryCountsStore( countsSnapshot );

        //THEN
        CountsSnapshot secondCountsSnapshot = countStore.snapshot( 3 );
        Assert.assertEquals( 3, secondCountsSnapshot.getTxId() );
        update.put( keyC, new long[]{1} );
        countStore.updateAll( 4, update );
        CountsSnapshot thirdCountsSnapshot = countStore.snapshot( 4 );
        Assert.assertEquals( 4, thirdCountsSnapshot.getTxId() );
    }
}
