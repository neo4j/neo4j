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

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;

import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.util.ArrayQueueOutOfOrderSequence;
import org.neo4j.kernel.impl.util.OutOfOrderSequence;
import org.neo4j.kernel.internal.DatabaseHealth;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.function.Predicates.awaitForever;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

public class InMemoryCountsStore implements CountsStore
{
    private static final long[] EMPTY_METADATA = {1L};
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    //TODO Always return long, not long[]. This requires splitting index keys into 4 keys, one for each value.
    private final ConcurrentHashMap<CountsKey,long[]> map;
    private final OutOfOrderSequence lastTxId = new ArrayQueueOutOfOrderSequence( BASE_TX_ID, 100, EMPTY_METADATA );
    private volatile CountsSnapshot snapshot;
    private final DatabaseHealth databaseHealth;

    public InMemoryCountsStore( CountsSnapshot snapshot, DatabaseHealth databaseHealth )
    {
        this.databaseHealth = databaseHealth;
        map = new ConcurrentHashMap<>( snapshot.getMap() );
        lastTxId.set( snapshot.getTxId(), EMPTY_METADATA );
    }

    public InMemoryCountsStore( DatabaseHealth databaseHealth )
    {
        this.databaseHealth = databaseHealth;
        this.map = new ConcurrentHashMap<>();
    }

    @Override
    public long[] get( CountsKey key )
    {
        return map.get( key );
    }

    @Override
    public void replace( CountsKey key, long[] replacement )
    {
        lock.readLock().lock();
        try
        {
            if ( snapshot != null )
            {
                throw new IllegalStateException(
                        "Cannot alter count store outside of a transaction while a snapshot is processing." );
            }
            map.put( key, replacement );
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    @Override
    public void update( CountsKey key, long[] delta )
    {
        lock.readLock().lock();
        try
        {
            if ( snapshot != null )
            {
                throw new IllegalStateException(
                        "Cannot alter count store outside of a transaction while a snapshot is processing." );
            }
            map.compute( key, ( k, v ) -> {
                if ( v == null )
                {
                    return Arrays.copyOf( delta, delta.length );
                }
                else
                {
                    return updateEachValue( v, delta );
                }
            } );
        }
        finally
        {
            lock.readLock().unlock();
        }
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
            if ( txId <= lastTxId.getHighestGapFreeNumber() )
            {
                throw new IllegalArgumentException( "Transaction ID " + txId + " has already been applied to the " +
                        "CountsStore. Highest Gap-Free number: " + lastTxId.getHighestGapFreeNumber() );
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
            awaitForever(
                    () -> ((lastTxId.getHighestGapFreeNumber() >= snapshot.getTxId()) && databaseHealth.isHealthy()),
                    100, MILLISECONDS );
            databaseHealth.assertHealthy( UnderlyingStorageException.class );
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

    @Override
    public boolean haveSeenTxId(long txId)
    {
        return lastTxId.seen( txId, EMPTY_METADATA );
    }

    @Override
    public void forEach( BiConsumer<CountsKey,long[]> action )
    {
        lock.writeLock().lock();
        map.forEach( action );
        lock.writeLock().unlock();
    }

    /**
     * This is essentially a deep copy of a map, necessary since our values in the map are long arrays. The crucial
     * part is the Arrays.copyOf() for the value.
     */
    private static ConcurrentHashMap<CountsKey,long[]> copyOfMap( Map<CountsKey,long[]> mapToCopy )
    {
        ConcurrentHashMap<CountsKey,long[]> newMap = new ConcurrentHashMap<>();
        mapToCopy.forEach( ( key, value ) -> newMap.put( key, Arrays.copyOf( value, value.length ) ) );
        return newMap;
    }
}