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

import org.neo4j.function.Function2;
import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.store.counts.keys.IndexCountsKey;
import org.neo4j.kernel.impl.store.counts.keys.IndexSampleKey;
import org.neo4j.kernel.impl.store.counts.keys.NodeKey;
import org.neo4j.kernel.impl.store.counts.keys.RelationshipKey;
import org.neo4j.kernel.impl.store.kvstore.KeyValueRecordVisitor;
import org.neo4j.kernel.impl.store.kvstore.SortedKeyValueStore;
import org.neo4j.register.ConcurrentRegisters;
import org.neo4j.register.Register.CopyableDoubleLongRegister;
import org.neo4j.register.Register.DoubleLong;
import org.neo4j.register.Register.DoubleLongRegister;

import static org.neo4j.register.Registers.newDoubleLongRegister;

class ConcurrentCountsTrackerState implements CountsTrackerState
{
    private static final int INITIAL_CHANGES_CAPACITY = 1024;

    private final SortedKeyValueStore<CountsKey, CopyableDoubleLongRegister> store;

    private final ConcurrentMap<CountsKey,CopyableDoubleLongRegister> changes =
        new ConcurrentHashMap<>( INITIAL_CHANGES_CAPACITY );

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
        return !changes.isEmpty();
    }

    @Override
    public DoubleLongRegister nodeCount( NodeKey nodeKey, DoubleLongRegister target )
    {
        return readIntoRegister( nodeKey, target );
    }

    @Override
    public void incrementNodeCount( NodeKey key, long delta )
    {
        CopyableDoubleLongRegister register = writeRegister( key );
        register.increment( 0, delta );
        assert register.satisfies( NON_NEGATIVE ) :
                String.format( "incrementNodeCount(key=%s, delta=%d) -> %s", key, delta, register );
    }

    @Override
    public DoubleLongRegister relationshipCount( RelationshipKey key, DoubleLongRegister target )
    {
        return readIntoRegister( key, target );
    }

    @Override
    public void incrementRelationshipCount( RelationshipKey key, long delta )
    {
        CopyableDoubleLongRegister register = writeRegister( key );
        register.increment( 0, delta );
        assert register.satisfies( NON_NEGATIVE ) :
                String.format( "incrementRelationshipCount(key=%s, delta=%d) -> %s", key, delta, register );
    }

    @Override
    public void replaceIndexUpdatesAndSize( IndexCountsKey key, long updates, long size )
    {
        assert updates >= 0 && size >= 0 :
                String.format( "replaceIndexSize(key=%s, updates=%d, size=%d)", key, updates, size );
        writeRegister( key ).write( updates, size );
    }

    @Override
    public DoubleLongRegister indexUpdatesAndSize( IndexCountsKey key, DoubleLongRegister target )
    {
        return readIntoRegister( key, target );
    }

    @Override
    public void incrementIndexUpdates( IndexCountsKey key, long delta )
    {
        assert delta >= 0 : String.format( "incrementIndexUpdates(key=%s, delta=%d)", key, delta );
        writeRegister( key ).increment( delta, 0l );
    }

    @Override
    public DoubleLongRegister indexSample( IndexSampleKey key, DoubleLongRegister target )
    {
        return readIntoRegister( key, target );
    }

    @Override
    public void replaceIndexSample( IndexSampleKey key, long unique, long size )
    {
        assert unique >= 0 && size >= 0 && unique <= size :
                String.format( "replaceIndexSample(key=%s, unique=%d, size=%d)", key, unique, size );
        writeRegister( key ).write( unique, size );
    }

    private DoubleLongRegister readIntoRegister( CountsKey key, DoubleLongRegister target )
    {
        CopyableDoubleLongRegister sample = changes.get( key );
        if ( sample == null )
        {
            store.get( key, target );
        }
        else
        {
            sample.copyTo( target );
        }
        return target;
    }

    private CopyableDoubleLongRegister writeRegister( CountsKey key )
    {
        CopyableDoubleLongRegister sample = changes.get( key );
        if ( sample == null )
        {
            sample = ConcurrentRegisters.OptimisticRead.newDoubleLongRegister();
            store.get( key, sample );
            CopyableDoubleLongRegister previous = changes.putIfAbsent( key, sample );
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
        try ( Merger<CountsKey> merger = new Merger<>( visitor, sortedUpdates( changes ) ) )
        {
            store.accept( merger, newDoubleLongRegister() );
        }
    }

    @Override
    public void close() throws IOException
    {
        store.close();
    }

    private static Update<CountsKey>[] sortedUpdates( ConcurrentMap<CountsKey,CopyableDoubleLongRegister> updates )
    {
        @SuppressWarnings( "unchecked" )
        Update<CountsKey>[] result = new Update[updates.size()];
        DoubleLongRegister tmp = newDoubleLongRegister();
        Iterator<Map.Entry<CountsKey,CopyableDoubleLongRegister>> entries = updates.entrySet().iterator();
        for ( int i = 0; i < result.length; i++ )
        {
            if ( !entries.hasNext() )
            {
                throw new ConcurrentModificationException( "fewer entries than expected" );
            }
            result[i] = Update.from( entries.next(), tmp );
        }
        if ( entries.hasNext() )
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
        private final CopyableDoubleLongRegister tmp = newDoubleLongRegister();
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
                for ( int i = next; i < updates.length; i++ )
                {
                    Update<K> update = updates[i];
                    update.writeTo( tmp );
                    target.visit( update.key, tmp );
                }
            }
        }
    }

    private static final class Update<K extends Comparable<K>> implements Comparable<Update<K>>
    {
        final K key;
        final long first;
        final long second;

        static <K extends Comparable<K>> Update<K> from( Map.Entry<K,CopyableDoubleLongRegister> entry,
                                                         DoubleLongRegister register )
        {
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

    private static final Function2<Long,Long,Boolean> NON_NEGATIVE = new Function2<Long,Long,Boolean>()
    {
        @Override
        public Boolean apply( Long first, Long second )
        {
            return first >= 0 && second >= 0;
        }
    };
}
