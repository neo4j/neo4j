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
package org.neo4j.kernel.impl.store.countStore;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.util.ArrayQueueOutOfOrderSequence;

public class CountStore
{
    private final ReadWriteLock lock;
    private final ConcurrentHashMap<CountsKey,long[]> map;
    private volatile ArrayQueueOutOfOrderSequence lastTxId;

    private Snapshot snapshot;

    public CountStore( Snapshot snapshot )
    {
        map = snapshot.getMap();
        lastTxId = new ArrayQueueOutOfOrderSequence( 0L, 100, new long[]{1} );
        lastTxId.set( snapshot.getTxId(), new long[]{} );
        lock = new ReentrantReadWriteLock();
    }

    public CountStore()
    {
        this( new Snapshot( 0, new ConcurrentHashMap<>() ) );
    }

    public long[] get( CountsKey key )
    {
        return map.get( key );
    }

    public void updateAll( long txId, Map<CountsKey,long[]> pairs )
    {
        lock.readLock().lock();
        try
        {
            pairs.forEach(
                    ( key, value ) -> map.compute( key, ( k, v ) -> v == null ? value : updateEachValue( v, value ) ) );
            lastTxId.offer( txId, new long[]{1} );

            if ( snapshot != null && snapshot.getTxId() >= txId )
            {
                pairs.forEach( ( key, value ) -> snapshot.getMap()
                        .compute( key, ( k, v ) -> v == null ? value : updateEachValue( v, value ) ) );
            }
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    private long[] updateEachValue( long[] v, long[] value )
    {
        for ( int i = 0; i < v.length; i++ )
        {
            v[i] = v[i] + value[i];
        }
        return v;
    }

    public Snapshot snapshot( long txId )
    {
        lock.writeLock().lock();
        try
        {
            snapshot = new Snapshot( Math.max( txId, lastTxId.highestEverSeen() ), CountStore.copyOfMap( this.map ) );
        }
        finally
        {
            lock.writeLock().unlock();
        }

        while ( lastTxId.getHighestGapFreeNumber() < snapshot.getTxId() )
        {
            Thread.yield();
        }

        lock.writeLock().lock();
        try
        {
            return snapshot;
        }

        finally
        {
            snapshot = null;
            lock.writeLock().unlock();
        }


    }

    public static ConcurrentHashMap<CountsKey,long[]> copyOfMap( ConcurrentHashMap<CountsKey,long[]> nextMap )
    {
        ConcurrentHashMap<CountsKey,long[]> newMap = new ConcurrentHashMap<>();
        nextMap.forEach( ( key, value ) -> newMap.put( key, Arrays.copyOf( value, value.length ) ) );
        return newMap;
    }
}