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

import org.neo4j.kernel.impl.api.CountsKey;
import org.neo4j.kernel.impl.store.kvstore.KeyValueRecordVisitor;
import org.neo4j.kernel.impl.store.kvstore.SortedKeyValueStore;
import org.neo4j.register.Register;
import org.neo4j.register.Registers;

class ConcurrentTrackerState implements CountsTracker.State
{
    private static final int INITIAL_CHANGES_CAPACITY = 1024;
    private final SortedKeyValueStore<CountsKey, Register.DoubleLongRegister> store;
    private final ConcurrentMap<CountsKey, AtomicLong> state = new ConcurrentHashMap<>( INITIAL_CHANGES_CAPACITY );

    ConcurrentTrackerState( SortedKeyValueStore<CountsKey, Register.DoubleLongRegister> store )
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
        return !state.isEmpty();
    }

    @Override
    public long getCount( CountsKey key )
    {
        /*
         * no need to copy values in the state since we delegate the caching to the page cache in CountStore.get(key)
         * moreover it will be faster to sort entries in the state if we do not add extra value when reading there
         * (see Merger)
         */
        final AtomicLong count = state.get( key );
        if ( count != null )
        {
            return count.get();
        }

        final Register.DoubleLongRegister value = Registers.newDoubleLongRegister();
        store.get( key, value );
        return value.readSecond();
    }

    @Override
    public long incrementCount( CountsKey key, long delta )
    {
        AtomicLong count = state.get( key );
        if ( count == null )
        {
            final Register.DoubleLongRegister value = Registers.newDoubleLongRegister();
            store.get( key, value );
            AtomicLong proposal = new AtomicLong( value.readSecond() );
            count = state.putIfAbsent( key, proposal );
            if ( count == null )
            {
                count = proposal;
            }
        }
        return count.addAndGet( delta );
    }

    @Override
    public void replaceCount( CountsKey key, long total )
    {
        AtomicLong count = state.get( key );
        if ( count == null )
        {
            count = new AtomicLong( total );
            state.put( key, count );
        }
        else
        {
            count.set( total );
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
    public CountsStore.Writer<CountsKey, Register.DoubleLongRegister> newWriter( File file, long lastCommittedTxId )
            throws IOException
    {
        return store.newWriter( file, lastCommittedTxId );
    }

    @Override
    public void accept( KeyValueRecordVisitor<CountsKey, Register.DoubleLongRegister> visitor )
    {
        try ( Merger<CountsKey> merger = new Merger<>( visitor, sortedUpdates( state ) ) )
        {
            store.accept( merger );
        }
    }

    @Override
    public void close() throws IOException
    {
        store.close();
    }

    private static Update<CountsKey>[] sortedUpdates( ConcurrentMap<CountsKey, AtomicLong> updates )
    {
        @SuppressWarnings( "unchecked" )
        Update<CountsKey>[] result = new Update[updates.size()];
        Iterator<Map.Entry<CountsKey, AtomicLong>> iterator = updates.entrySet().iterator();
        for ( int i = 0; i < result.length; i++ )
        {
            if ( !iterator.hasNext() )
            {
                throw new ConcurrentModificationException( "fewer entries than expected" );
            }
            result[i] = new Update<>( iterator.next() );
        }
        if ( iterator.hasNext() )
        {
            throw new ConcurrentModificationException( "more entries than expected" );
        }
        Arrays.sort( result );
        return result;
    }

    private static final class Merger<K extends Comparable<K>> implements KeyValueRecordVisitor<K, Register.DoubleLongRegister>,
            AutoCloseable
    {
        private final KeyValueRecordVisitor<K, Register.DoubleLongRegister> target;
        private final Update<K>[] updates;
        private int next;
        private final Register.DoubleLongRegister valueRegister;

        public Merger( KeyValueRecordVisitor<K, Register.DoubleLongRegister> target, Update<K>[] updates )
        {
            this.target = target;
            this.updates = updates;
            this.valueRegister = target.valueRegister();
        }

        @Override
        public Register.DoubleLongRegister valueRegister()
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
                    valueRegister.write( 0, nextUpdate.value );
                }
                else if ( cmp > 0 )
                { // write this before writing the entry from the store
                    next++;
                    long originalFirst = valueRegister.readFirst();
                    long originalSecond = valueRegister.readSecond();
                    valueRegister.write( 0, nextUpdate.value );
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
                valueRegister.write( 0, updates[i].value );
                target.visit( updates[i].key );
            }
        }
    }

    private static final class Update<K extends Comparable<K>> implements Comparable<Update<K>>
    {
        final K key;
        final long value;

        Update( Map.Entry<K, AtomicLong> entry )
        {
            this( entry.getKey(), entry.getValue().longValue() );
        }

        Update( K key, long value )
        {
            this.key = key;
            this.value = value;
        }

        @Override
        public String toString()
        {
            return String.format( "Update{key=%s, value=%d}", key, value );
        }

        @Override
        public int compareTo( Update<K> that )
        {
            return this.key.compareTo( that.key );
        }
    }
}
