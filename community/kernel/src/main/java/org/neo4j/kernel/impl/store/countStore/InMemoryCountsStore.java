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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.neo4j.function.Predicates;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.util.ArrayQueueOutOfOrderSequence;
import org.neo4j.kernel.impl.util.OutOfOrderSequence;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class InMemoryCountsStore implements CountsStore
{
    private static final long[] EMPTY_METADATA = {1L};
    private final ReadWriteLock lock;
    private final ConcurrentHashMap<CountsKey,long[]> map;
    //TODO Always return long, not long[]. This requires splitting index keys into 4 keys, one for each value.

    private final OutOfOrderSequence lastTxId;
    private CountsSnapshot snapshot;

    public InMemoryCountsStore( CountsSnapshot snapshot )
    {
        map = new ConcurrentHashMap<>( snapshot.getMap() );
        lastTxId = new ArrayQueueOutOfOrderSequence( 0L, 100, EMPTY_METADATA );
        lastTxId.set( snapshot.getTxId(), EMPTY_METADATA );
        lock = new ReentrantReadWriteLock();
    }

    public InMemoryCountsStore()
    {
        this( new CountsSnapshot( 0, new ConcurrentHashMap<>() ) );
    }

    @Override
    public long[] get( CountsKey key )
    {
        return map.get( key );
    }

    @Override
    public void updateAll( long txId, Map<CountsKey,long[]> pairs )
    {
        lock.readLock().lock();
        try
        {
            applyUpdates( pairs, map );
            if ( snapshot != null && snapshot.getTxId() >= txId )
            {
                applyUpdates( pairs, snapshot.getMap() );
            }
            lastTxId.offer( txId, EMPTY_METADATA );
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    private void applyUpdates( Map<CountsKey,long[]> updates, Map<CountsKey,long[]> map )
    {
        updates.forEach(
                ( key, value ) -> map.compute( key, ( k, v ) -> v == null ? value : updateEachValue( v, value ) ) );
    }

    private long[] updateEachValue( long[] v, long[] value )
    {
        for ( int i = 0; i < v.length; i++ )
        {
            v[i] = v[i] + value[i];
        }
        return v;
    }

    /**
     * This method is thread safe w.r.t updates to the countstore, but not for performing concurrent snapshots.
     */
    @Override
    public CountsSnapshot snapshot( long txId )
    {
        if ( snapshot != null )
        {
            throw new IllegalStateException( "Cannot perform snapshot while another snapshot is processing." );
        }

        lock.writeLock().lock();
        try
        {
            snapshot = new CountsSnapshot( Math.max( txId, lastTxId.highestEverSeen() ),
                    InMemoryCountsStore.copyOfMap( this.map ) );
        }
        finally
        {
            lock.writeLock().unlock();
        }

        try
        {
            Predicates
                    .awaitForever( () -> lastTxId.getHighestGapFreeNumber() >= snapshot.getTxId(), 100, MILLISECONDS );
            return snapshot;
        }
        catch ( InterruptedException ex )
        {
            throw Exceptions
                    .withCause( new UnderlyingStorageException( "Construction of snapshot was interrupted." ), ex );
        }
        finally
        {
            snapshot = null;
        }
    }

    private static HashMap<CountsKey,long[]> copyOfMap( Map<CountsKey,long[]> nextMap )
    {
        HashMap<CountsKey,long[]> newMap = new HashMap<>();
        nextMap.forEach( ( key, value ) -> newMap.put( key, Arrays.copyOf( value, value.length ) ) );
        return newMap;
    }
}