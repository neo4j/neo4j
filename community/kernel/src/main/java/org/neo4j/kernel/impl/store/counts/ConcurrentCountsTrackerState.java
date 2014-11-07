/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.kernel.impl.store.counts.CountsKey.IndexCountsKey;
import org.neo4j.kernel.impl.store.kvstore.KeyValueRecordVisitor;
import org.neo4j.kernel.impl.store.kvstore.SortedKeyValueStore;
import org.neo4j.register.ConcurrentRegisters;
import org.neo4j.register.Register.CopyableDoubleLongRegister;
import org.neo4j.register.Register.DoubleLong;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.register.Registers;

import static org.neo4j.kernel.impl.store.counts.CountsKey.IndexSampleKey;
import static org.neo4j.kernel.impl.store.counts.CountsKey.NodeKey;
import static org.neo4j.kernel.impl.store.counts.CountsKey.RelationshipKey;

class ConcurrentCountsTrackerState implements CountsTrackerState
{
    private static final int INITIAL_CHANGES_CAPACITY = 1024;
    private static final int INITIAL_INDICES_CAPACITY = 32;

    private final SortedKeyValueStore<CountsKey, CopyableDoubleLongRegister> store;

    private final ConcurrentMap<CountsKey, AtomicLong> counts =
        new ConcurrentHashMap<>( INITIAL_CHANGES_CAPACITY );

    private final ConcurrentMap<CountsKey,CopyableDoubleLongRegister> samples =
        new ConcurrentHashMap<>( INITIAL_INDICES_CAPACITY );

    ConcurrentCountsTrackerState( SortedKeyValueStore<CountsKey, CopyableDoubleLongRegister> store )
    {
        this.store = store;
    }

    @Override
    public String toString()
    {
        return String.format( "ConcurrentTrackerState[store=%s - %s]", store, super.toString() );
    }

    public boolean hasChanges()
    {
        return !(counts.isEmpty() && samples.isEmpty());
    }

    @Override
    public long nodeCount( NodeKey nodeKey )
    {
        return readCount( nodeKey );
    }

    @Override
    public void incrementNodeCount( NodeKey nodeKey, long delta )
    {
        incrementCount( nodeKey, delta );
    }

    @Override
    public long relationshipCount( RelationshipKey relationshipKey )
    {
        return readCount( relationshipKey );
    }

    @Override
    public void incrementRelationshipCount( RelationshipKey relationshipKey, long delta )
    {
        incrementCount( relationshipKey, delta );
    }

    @Override
    public void replaceIndexUpdatesAndSize( IndexCountsKey indexCountsKey, long updates, long size )
    {
        writeRegister( indexCountsKey ).write( updates, size );
    }

    @Override
    public void indexUpdatesAndSize( IndexCountsKey indexCountsKey, DoubleLongRegister target )
    {
        readIntoRegister( indexCountsKey, target );
    }

    @Override
    public void incrementIndexUpdates( IndexCountsKey indexCountsKey, long delta )
    {
        writeRegister( indexCountsKey ).increment( delta, 0l );
    }

    @Override
    public void indexSample( IndexSampleKey indexSampleKey, DoubleLongRegister target )
    {
        readIntoRegister( indexSampleKey, target );
    }

    @Override
    public void replaceIndexSample( IndexSampleKey indexSampleKey, long unique, long size )
    {
        writeRegister( indexSampleKey ).write( unique, size );
    }

    private long readCount( CountsKey key )
    {
        /*
         * no need to copy values in the state since we delegate the caching to the page cache in CountStore.get(key)
         * moreover it will be faster to sort entries in the state if we do not add extra value when reading there
         * (see Merger)
         */
        final AtomicLong count = counts.get( key );
        if ( count != null )
        {
            return count.get();
        }

        final DoubleLongRegister value = Registers.newDoubleLongRegister();
        store.get( key, value );
        return value.readSecond();
    }

    private long incrementCount( CountsKey key, long delta )
    {
        AtomicLong count = counts.get( key );
        if ( count == null )
        {
            final DoubleLongRegister value = Registers.newDoubleLongRegister();
            store.get( key, value );
            AtomicLong proposal = new AtomicLong( value.readSecond() );
            count = counts.putIfAbsent( key, proposal );
            if ( count == null )
            {
                count = proposal;
            }
        }
        return count.addAndGet( delta );
    }

    private void readIntoRegister( CountsKey key, DoubleLongRegister target )
    {
        CopyableDoubleLongRegister sample = samples.get( key );
        if ( sample == null )
        {
            store.get( key, target );
        }
        else
        {
            sample.copyTo( target );
        }
    }

    private DoubleLong.Out writeRegister( CountsKey key )
    {
        CopyableDoubleLongRegister sample = samples.get( key );
        if ( sample == null )
        {
            sample = ConcurrentRegisters.OptimisticRead.newDoubleLongRegister();
            store.get( key, sample );
            CopyableDoubleLongRegister previous = samples.putIfAbsent( key, sample );
            return previous == null ? sample : previous;
        }
        return sample;
    }

    @Override
    public File storeFile()
    {
        return store.file();
    }

    @Override
    public long lastTxId()
    {
        return store.lastTxId();
    }

    @Override
    public CountsStore.Writer<CountsKey, CopyableDoubleLongRegister> newWriter( File file, long lastCommittedTxId )
            throws IOException
    {
        return store.newWriter( file, lastCommittedTxId );
    }

    @Override
    public void accept( KeyValueRecordVisitor<CountsKey, CopyableDoubleLongRegister> visitor )
    {
        try ( Merger<CountsKey> merger = new Merger<>( visitor, sortedUpdates( counts, samples ) ) )
        {
            store.accept( merger, Registers.newDoubleLongRegister() );
        }
    }

    @Override
    public void close() throws IOException
    {
        store.close();
    }

    private static Update<CountsKey>[] sortedUpdates( ConcurrentMap<CountsKey, AtomicLong> singleUpdates,
                                                    ConcurrentMap<CountsKey, CopyableDoubleLongRegister> doubleUpdates )
    {
        int singleSize = singleUpdates.size();
        int doubleSize = doubleUpdates.size();

        @SuppressWarnings( "unchecked" )
        Update<CountsKey>[] result = new Update[singleSize + doubleSize];

        Iterator<Map.Entry<CountsKey, AtomicLong>> singleIterator = singleUpdates.entrySet().iterator();
        for ( int i = 0; i < singleSize; i++ )
        {
            if ( !singleIterator.hasNext() )
            {
                throw new ConcurrentModificationException( "fewer entries than expected" );
            }
            result[i] = Update.fromSingleLongEntry( singleIterator.next() );
        }
        if ( singleIterator.hasNext() )
        {
            throw new ConcurrentModificationException( "more entries than expected" );
        }

        Iterator<Map.Entry<CountsKey, CopyableDoubleLongRegister>> doubleIterator = doubleUpdates.entrySet().iterator();
        for ( int i = singleSize; i < result.length; i++ )
        {
            if ( !doubleIterator.hasNext() )
            {
                throw new ConcurrentModificationException( "fewer entries than expected" );
            }
            result[i] = Update.fromDoubleLongEntry( doubleIterator.next() );
        }
        if ( doubleIterator.hasNext() )
        {
            throw new ConcurrentModificationException( "more entries than expected" );
        }

        Arrays.sort( result );
        return result;
    }

    private static final class Merger<K extends Comparable<K>>
            implements KeyValueRecordVisitor<K, CopyableDoubleLongRegister>, AutoCloseable
    {
        private final KeyValueRecordVisitor<K, CopyableDoubleLongRegister> target;
        private final CopyableDoubleLongRegister tmp = Registers.newDoubleLongRegister();
        private final Update<K>[] updates;
        private int next;

        public Merger( KeyValueRecordVisitor<K, CopyableDoubleLongRegister> target, Update<K>[] updates )
        {
            this.target = target;
            this.updates = updates;
        }

        @Override
        public void visit( K key, CopyableDoubleLongRegister register  )
        {
            while ( next < updates.length )
            {
                Update<K> nextUpdate = updates[next];
                int cmp = key.compareTo( nextUpdate.key );
                if ( cmp == 0 )
                { // overwrite the value in the store
                    next++;
                    nextUpdate.writeTo( register );
                }
                else if ( cmp > 0 )
                { // write this before writing the entry from the store
                    next++;
                    register.copyTo( tmp );
                    nextUpdate.writeTo( register );
                    target.visit( nextUpdate.key, register );
                    tmp.copyTo( register );
                    continue; // then see if there are more entries to consider from the updates...
                }
                break;
            }
            target.visit( key, register );
        }

        public void close()
        {
            if ( next < updates.length)
            {
                DoubleLongRegister register = Registers.newDoubleLongRegister();
                for ( int i = next; i < updates.length; i++ )
                {
                    Update<K> update = updates[i];
                    update.writeTo( register );
                    target.visit( update.key, register );
                }
            }
        }
    }

    private static final class Update<K extends Comparable<K>> implements Comparable<Update<K>>
    {
        final K key;
        final long first;
        final long second;

        static <K extends Comparable<K>> Update<K> fromSingleLongEntry( Map.Entry<K, AtomicLong> entry )
        {
            return new Update<>( entry.getKey(), 0, entry.getValue().longValue() );
        }

        static <K extends Comparable<K>> Update<K> fromDoubleLongEntry( Map.Entry<K, CopyableDoubleLongRegister> entry )
        {
            // read out atomically in case the entry value is a concurrent register
            DoubleLongRegister register = Registers.newDoubleLongRegister();
            entry.getValue().copyTo( register );
            return new Update<>( entry.getKey(), register.readFirst(), register.readSecond() );
        }

        Update( K key, long first, long second )
        {
            this.key = key;
            this.first = first;
            this.second = second;
        }

        void writeTo( DoubleLong.Out target )
        {
            target.write( first, second );
        }

        @Override
        public String toString()
        {
            return String.format( "Update{key=%s, first=%d, second=%d}", key, first, second );
        }

        @Override
        public int compareTo( Update<K> that )
        {
            return this.key.compareTo( that.key );
        }
    }
}
