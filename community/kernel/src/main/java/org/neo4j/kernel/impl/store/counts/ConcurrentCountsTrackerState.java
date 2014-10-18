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

import org.neo4j.kernel.impl.store.kvstore.KeyValueRecordVisitor;
import org.neo4j.kernel.impl.store.kvstore.SortedKeyValueStore;
import org.neo4j.register.ConcurrentRegisters;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.register.Registers;

import static org.neo4j.kernel.impl.store.counts.CountsKey.IndexSampleKey;
import static org.neo4j.kernel.impl.store.counts.CountsKey.IndexSizeKey;
import static org.neo4j.kernel.impl.store.counts.CountsKey.NodeKey;
import static org.neo4j.kernel.impl.store.counts.CountsKey.RelationshipKey;

class ConcurrentCountsTrackerState implements CountsTrackerState
{
    private static final int INITIAL_CHANGES_CAPACITY = 1024;
    private static final int INITIAL_INDICES_CAPACITY = 32;

    private final SortedKeyValueStore<CountsKey, DoubleLongRegister> store;

    private final ConcurrentMap<CountsKey, AtomicLong> counts =
        new ConcurrentHashMap<>( INITIAL_CHANGES_CAPACITY );

    private final ConcurrentMap<CountsKey, DoubleLongRegister> samples =
        new ConcurrentHashMap<>( INITIAL_INDICES_CAPACITY );

    ConcurrentCountsTrackerState( SortedKeyValueStore<CountsKey, DoubleLongRegister> store )
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
        return getCount( nodeKey );
    }

    @Override
    public long incrementNodeCount( NodeKey nodeKey, long delta )
    {
        return incrementCount( nodeKey, delta );
    }

    @Override
    public long relationshipCount( RelationshipKey relationshipKey )
    {
        return getCount( relationshipKey );
    }

    @Override
    public boolean indexSample( IndexSampleKey indexSampleKey, DoubleLongRegister target )
    {
        return getSample( indexSampleKey, target );
    }

    @Override
    public long incrementRelationshipCount( RelationshipKey relationshipKey, long delta )
    {
        return incrementCount( relationshipKey, delta );
    }

    @Override
    public long indexSizeCount( IndexSizeKey indexSizeKey )
    {
        return getCount( indexSizeKey );
    }

    @Override
    public long incrementIndexSizeCount( IndexSizeKey indexSizeKey, long delta )
    {
        return incrementCount( indexSizeKey, delta );
    }

    @Override
    public void replaceIndexSizeCount( IndexSizeKey indexSizeKey, long total )
    {
        replaceCount( indexSizeKey, total );
    }

    @Override
    public void replaceIndexSample( IndexSampleKey indexSampleKey, long unique, long size )
    {
        replaceSample( indexSampleKey, unique, size );
    }

    private long getCount( CountsKey key )
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

    private void replaceCount( CountsKey key, long total )
    {
        AtomicLong count = counts.get( key );
        if ( count == null )
        {
            count = new AtomicLong( total );
            counts.put( key, count );
        }
        else
        {
            count.set( total );
        }
    }

    private boolean getSample( CountsKey key, DoubleLongRegister target )
    {
        DoubleLongRegister sample = samples.get( key );
        if ( sample == null )
        {
            store.get( key, target );
            return true;
        }
        else
        {
            sample.copyTo( target );
            return true;
        }
    }

    private void replaceSample( CountsKey key, long first, long second )
    {
        DoubleLongRegister sample = samples.get( key );
        if ( sample == null )
        {
            sample = ConcurrentRegisters.OptimisticRead.newDoubleLongRegister( first, second );
            samples.put( key, sample );
        }
        else
        {
            sample.write( first, second );
        }
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
    public CountsStore.Writer<CountsKey, DoubleLongRegister> newWriter( File file, long lastCommittedTxId )
            throws IOException
    {
        return store.newWriter( file, lastCommittedTxId );
    }

    @Override
    public void accept( KeyValueRecordVisitor<CountsKey, DoubleLongRegister> visitor )
    {
        try ( Merger<CountsKey> merger = new Merger<>( visitor, sortedUpdates( counts, samples ) ) )
        {
            store.accept( merger );
        }
    }

    @Override
    public void close() throws IOException
    {
        store.close();
    }

    private static Update<CountsKey>[] sortedUpdates( ConcurrentMap<CountsKey, AtomicLong> singleUpdates,
                                                      ConcurrentMap<CountsKey, DoubleLongRegister> doubleUpdates )
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

        Iterator<Map.Entry<CountsKey, DoubleLongRegister>> doubleIterator = doubleUpdates.entrySet().iterator();
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

    private static final class Merger<K extends Comparable<K>> implements KeyValueRecordVisitor<K, DoubleLongRegister>,
            AutoCloseable
    {
        private final KeyValueRecordVisitor<K, DoubleLongRegister> target;
        private final Update<K>[] updates;
        private int next;
        private final DoubleLongRegister valueRegister;

        public Merger( KeyValueRecordVisitor<K, DoubleLongRegister> target, Update<K>[] updates )
        {
            this.target = target;
            this.updates = updates;
            this.valueRegister = target.valueRegister();
        }

        @Override
        public DoubleLongRegister valueRegister()
        {
            return valueRegister;
        }

        @Override
        public void visit( K key )
        {
            while ( next < updates.length )
            {
                Update<K> nextUpdate = updates[next];
                int cmp = key.compareTo( nextUpdate.key );
                if ( cmp == 0 )
                { // overwrite the value in the store
                    next++;
                    nextUpdate.writeTo( valueRegister );
                }
                else if ( cmp > 0 )
                { // write this before writing the entry from the store
                    next++;
                    long originalFirst = valueRegister.readFirst();
                    long originalSecond = valueRegister.readSecond();
                    nextUpdate.writeTo( valueRegister );
                    target.visit( nextUpdate.key );
                    valueRegister.write( originalFirst, originalSecond );
                    continue; // then see if there are more entries to consider from the updates...
                }
                break;
            }
            target.visit( key );
        }

        public void close()
        {
            for ( int i = next; i < updates.length; i++ )
            {
                Update<K> update = updates[i];
                update.writeTo( valueRegister );
                target.visit( update.key );
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

        static <K extends Comparable<K>> Update<K> fromDoubleLongEntry( Map.Entry<K, DoubleLongRegister> entry )
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

        void writeTo( DoubleLongRegister target )
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
