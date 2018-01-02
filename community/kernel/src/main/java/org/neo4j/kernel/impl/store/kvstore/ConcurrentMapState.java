/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.kvstore;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

class ConcurrentMapState<Key> extends ActiveState<Key>
{
    private final ConcurrentMap<Key, byte[]> changes;
    private final File file;
    private final AtomicLong highestAppliedVersion;
    private final AtomicLong appliedChanges;
    private final AtomicBoolean hasTrackedChanges;
    private final long previousVersion;

    ConcurrentMapState( ReadableState<Key> store, File file )
    {
        super( store );
        this.previousVersion = store.version();
        this.file = file;
        this.highestAppliedVersion = new AtomicLong( previousVersion );
        this.changes = new ConcurrentHashMap<>();
        this.appliedChanges = new AtomicLong();
        hasTrackedChanges = new AtomicBoolean();
    }

    private ConcurrentMapState( Prototype<Key> prototype, ReadableState<Key> store, File file )
    {
        super( store );
        this.previousVersion = store.version();
        this.file = file;
        this.hasTrackedChanges = prototype.hasTrackedChanges;
        this.changes = prototype.changes;
        this.highestAppliedVersion = prototype.highestAppliedVersion;
        this.appliedChanges = prototype.appliedChanges;
    }

    @Override
    public String toString()
    {
        return super.toString() + "[" + file + "]";
    }

    @Override
    public EntryUpdater<Key> updater( long version, Lock lock )
    {
        if ( version <= previousVersion )
        {
            return EntryUpdater.noUpdates();
        }
        update( highestAppliedVersion, version );
        hasTrackedChanges.set( true );
        return new Updater<>( lock, store, changes, appliedChanges );
    }

    @Override
    public EntryUpdater<Key> unsafeUpdater( Lock lock )
    {
        hasTrackedChanges.set( true );
        return new Updater<>( lock, store, changes, null );
    }

    private static class Updater<Key> extends EntryUpdater<Key>
    {
        private AtomicLong changeCounter;
        private final ReadableState<Key> store;
        private final ConcurrentMap<Key, byte[]> changes;

        Updater( Lock lock, ReadableState<Key> store, ConcurrentMap<Key, byte[]> changes, AtomicLong changeCounter )
        {
            super( lock );
            this.changeCounter = changeCounter;
            this.store = store;
            this.changes = changes;
        }

        @Override
        public void apply( Key key, ValueUpdate update ) throws IOException
        {
            ensureOpenOnSameThread();
            applyUpdate( store, changes, key, update, false );
        }

        @Override
        public void close()
        {
            if ( changeCounter != null )
            {
                changeCounter.incrementAndGet();
                changeCounter = null;
            }
            super.close();
        }
    }

    @Override
    protected long storedVersion()
    {
        return previousVersion;
    }

    @Override
    protected EntryUpdater<Key> resettingUpdater( Lock lock, final Runnable closeAction )
    {
        if ( hasChanges() )
        {
            throw new IllegalStateException( "Cannot reset when there are changes!" );
        }
        return new EntryUpdater<Key>( lock )
        {
            @Override
            public void apply( Key key, ValueUpdate update ) throws IOException
            {
                ensureOpen();
                applyUpdate( store, changes, key, update, true );
            }

            @Override
            public void close()
            {
                try
                {
                    closeAction.run();
                }
                finally
                {
                    super.close();
                }
            }
        };
    }

    @Override
    protected PrototypeState<Key> prototype( long version )
    {
        return new Prototype<>( this, version );
    }

    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    static <Key> void applyUpdate( ReadableState<Key> store, ConcurrentMap<Key, byte[]> changes,
                                   Key key, ValueUpdate update, boolean reset ) throws IOException
    {
        byte[] value = changes.get( key );
        if ( value == null )
        {
            final byte[] proposal = new byte[store.keyFormat().valueSize()];
            synchronized ( proposal )
            {
                value = changes.putIfAbsent( key, proposal );
                if ( value == null )
                {
                    BigEndianByteArrayBuffer buffer = new BigEndianByteArrayBuffer( proposal );
                    if ( !reset )
                    {
                        PreviousValue lookup = new PreviousValue( proposal );
                        if ( !store.lookup( key, lookup ) )
                        {
                            buffer.clear();
                        }
                    }
                    update.update( buffer );
                    return;
                }
            }
        }
        synchronized ( value )
        {
            BigEndianByteArrayBuffer target = new BigEndianByteArrayBuffer( value );
            if ( reset )
            {
                target.clear();
            }
            update.update( target );
        }
    }

    private static void update( AtomicLong highestAppliedVersion, long version )
    {
        for ( long high; ; )
        {
            high = highestAppliedVersion.get();
            if ( version <= high )
            {
                return;
            }
            if ( highestAppliedVersion.compareAndSet( high, version ) )
            {
                return;
            }
        }
    }

    private static class Prototype<Key> extends PrototypeState<Key>
    {
        final ConcurrentMap<Key, byte[]> changes = new ConcurrentHashMap<>();
        final AtomicLong highestAppliedVersion, appliedChanges = new AtomicLong();
        final AtomicBoolean hasTrackedChanges;
        private final long threshold;

        Prototype( ConcurrentMapState<Key> state, long version )
        {
            super( state );
            threshold = version;
            hasTrackedChanges = new AtomicBoolean();
            this.highestAppliedVersion = new AtomicLong( version );
        }

        @Override
        protected ActiveState<Key> create( ReadableState<Key> sub, File file )
        {
            return new ConcurrentMapState<>( this, sub, file );
        }

        @Override
        protected EntryUpdater<Key> updater( long version, Lock lock )
        {
            update( highestAppliedVersion, version );
            if ( version > threshold )
            {
                hasTrackedChanges.set( true );
                return new Updater<>( lock, store, changes, appliedChanges );
            }
            else
            {
                return new Updater<>( lock, store, changes, null );
            }
        }

        @Override
        protected EntryUpdater<Key> unsafeUpdater( Lock lock )
        {
            hasTrackedChanges.set( true );
            return new Updater<>( lock, store, changes, null );
        }

        @Override
        protected boolean hasChanges()
        {
            return hasTrackedChanges.get() && !changes.isEmpty();
        }

        @Override
        protected long version()
        {
            return highestAppliedVersion.get();
        }

        @Override
        protected boolean lookup( Key key, ValueSink sink ) throws IOException
        {
            return performLookup( store, changes, key, sink );
        }

        @Override
        protected DataProvider dataProvider() throws IOException
        {
            return ConcurrentMapState.dataProvider( store, changes );
        }
    }

    private static class PreviousValue extends ValueSink
    {
        private final byte[] proposal;

        PreviousValue( byte[] proposal )
        {
            this.proposal = proposal;
        }

        @Override
        protected void value( ReadableBuffer value )
        {
            value.get( 0, proposal );
        }
    }

    @Override
    protected long version()
    {
        return highestAppliedVersion.get();
    }

    @Override
    protected long applied()
    {
        return appliedChanges.get();
    }

    @Override
    protected boolean hasChanges()
    {
        return hasTrackedChanges.get() && !changes.isEmpty();
    }

    @Override
    protected void close() throws IOException
    {
        store.close();
    }

    @Override
    protected File file()
    {
        return file;
    }

    @Override
    protected Factory factory()
    {
        return State.Strategy.CONCURRENT_HASH_MAP;
    }

    @Override
    protected boolean lookup( Key key, ValueSink sink ) throws IOException
    {
        return performLookup( store, changes, key, sink );
    }

    private static <Key> boolean performLookup( ReadableState<Key> store, ConcurrentMap<Key, byte[]> changes,
                                                Key key, ValueSink sink ) throws IOException
    {
        byte[] value = changes.get( key );
        if ( value != null )
        {
            sink.value( new BigEndianByteArrayBuffer( value ) );
            return true;
        }
        return store.lookup( key, sink );
    }

    /**
     * This method is expected to be called under a lock preventing modification to the state.
     */
    @Override
    public DataProvider dataProvider() throws IOException
    {
        return dataProvider( store, changes );
    }

    private static <Key> DataProvider dataProvider( ReadableState<Key> store, ConcurrentMap<Key, byte[]> changes )
            throws IOException
    {
        if ( changes.isEmpty() )
        {
            return store.dataProvider();
        }
        else
        {
            KeyFormat<Key> keys = store.keyFormat();
            return new KeyValueMerger( store.dataProvider(), new UpdateProvider(
                    sortedUpdates( keys, changes ) ), keys.keySize(), keys.valueSize() );
        }
    }

    private static <Key> byte[][] sortedUpdates( KeyFormat<Key> keys, ConcurrentMap<Key, byte[]> changes )
    {
        Entry[] buffer = new Entry[changes.size()];
        Iterator<Map.Entry<Key, byte[]>> entries = changes.entrySet().iterator();
        for ( int i = 0; i < buffer.length; i++ )
        {
            Map.Entry<Key, byte[]> next = entries.next(); // we hold the lock, so this should succeed
            byte[] key = new byte[keys.keySize()];
            keys.writeKey( next.getKey(), new BigEndianByteArrayBuffer( key ) );
            buffer[i] = new Entry( key, next.getValue() );
        }
        Arrays.sort( buffer );
        assert !entries.hasNext() : "We hold the lock, so we should see 'size' entries.";
        byte[][] result = new byte[buffer.length * 2][];
        for ( int i = 0; i < buffer.length; i++ )
        {
            result[i * 2] = buffer[i].key;
            result[i * 2 + 1] = buffer[i].value;
        }
        return result;
    }

    private static class Entry implements Comparable<Entry>
    {
        final byte[] key, value;

        private Entry( byte[] key, byte[] value )
        {
            this.key = key;
            this.value = value;
        }

        @Override
        public int compareTo( Entry that )
        {
            return BigEndianByteArrayBuffer.compare( this.key, that.key, 0 );
        }
    }

    private static class UpdateProvider implements DataProvider
    {
        private final byte[][] data;
        private int i;

        UpdateProvider( byte[][] data )
        {
            this.data = data;
        }

        @Override
        public boolean visit( WritableBuffer key, WritableBuffer value ) throws IOException
        {
            if ( i < data.length )
            {
                key.put( 0, data[i] );
                value.put( 0, data[i + 1] );
                i += 2;
                return true;
            }
            return false;
        }

        @Override
        public void close() throws IOException
        {
        }
    }
}
