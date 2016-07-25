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

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.util.ArrayQueueOutOfOrderSequence;
import org.neo4j.kernel.impl.util.OutOfOrderSequence;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.function.Predicates.awaitForever;

public class InMemoryCountsStore implements CountsStore
{
    private static final long[] EMPTY_METADATA = {1L};
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    //TODO Always return long, not long[]. This requires splitting index keys into 4 keys, one for each value.
    private final ConcurrentHashMap<CountsKey,long[]> map;
    private final OutOfOrderSequence lastTxId = new ArrayQueueOutOfOrderSequence( 0L, 100, EMPTY_METADATA );
    private CountsSnapshot snapshot;

    public InMemoryCountsStore( CountsSnapshot snapshot )
    {
        map = new ConcurrentHashMap<>( snapshot.getMap() );
        lastTxId.set( snapshot.getTxId(), EMPTY_METADATA );
    }

    public InMemoryCountsStore()
    {
        map = new ConcurrentHashMap<>();
        lastTxId.set( 0, EMPTY_METADATA );
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

    /**
     * For each key in the updates param, applies it's value as a diff to the cooresponding value in the given map.
     *
     * @param updates A map containing the diffs to apply to the corresponding values in the map.
     * @param map The map to be updated.
     */
    private void applyUpdates( Map<CountsKey,long[]> updates, Map<CountsKey,long[]> map )
    {
        updates.forEach( ( key, value ) -> map.compute( key, ( k, v ) -> {
            if ( v == null )
            {
                return Arrays.copyOf( value, value.length );
            }
            else
            {
                return updateEachValue( v, value );
            }
        } ) );
    }

    /**
     * Because IndexSampleKey and IndexStatisticsKey have 2 values and NodeKey and RelationshipKey have only 1 value,
     * these keys are stored in the same map by making the value an array of longs. Therefore, when applying the diff
     * to the keys values, this loop works for keys of both types. In the future, we should separate the values in
     * IndexSample and IndexStatistics into multiple keys each with one value.
     */
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
        lock.writeLock().lock();
        try
        {
            if ( snapshot != null )
            {
                throw new IllegalStateException( "Cannot perform snapshot while another snapshot is processing." );
            }
            long snapshotTxId = Math.max( txId, lastTxId.highestEverSeen() );
            snapshot = new CountsSnapshot( snapshotTxId, copyOfMap( this.map ) );
        }
        finally
        {
            lock.writeLock().unlock();
        }

        try
        {
            //TODO We should NOT blindly wait forever. We also shouldn't have a timeout. The proposed solution is to
            // wait forever unless the database has failed in the background. In that case we want to fail as well.
            // This will allow us to not timeout prematurely but also not hang when the database is broken.
            awaitForever( () -> lastTxId.getHighestGapFreeNumber() >= snapshot.getTxId(), 100, MILLISECONDS );
            return snapshot;
        }
        catch ( InterruptedException ex )
        {
            Thread.currentThread().interrupt();
            throw Exceptions
                    .withCause( new UnderlyingStorageException( "Construction of snapshot was interrupted." ), ex );
        }
        finally
        {
            snapshot = null;
        }
    }

    /**
     * This is essentially a deep copy of a map, necessary since our values in the map are long arrays. The crucial
     * part is the Arrays.copyOf() for the value.
     */
    private static Map<CountsKey,long[]> copyOfMap( Map<CountsKey,long[]> mapToCopy )
    {
        ConcurrentHashMap<CountsKey,long[]> newMap = new ConcurrentHashMap<>();
        mapToCopy.forEach( ( key, value ) -> newMap.put( key, Arrays.copyOf( value, value.length ) ) );
        return newMap;
    }
}
