/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.FileIsNotMappedException;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Logger;

import static org.neo4j.kernel.impl.store.kvstore.LockWrapper.readLock;
import static org.neo4j.kernel.impl.store.kvstore.LockWrapper.writeLock;

/**
 * The base for building a key value store based on rotating immutable
 * {@linkplain KeyValueStoreFile key/value store files}
 *
 * @param <Key> a base type for the keys stored in this store.
 */
@Rotation( Rotation.Strategy.LEFT_RIGHT )
@State( State.Strategy.CONCURRENT_HASH_MAP )
public abstract class AbstractKeyValueStore<Key> extends LifecycleAdapter
{
    private final UpdateLock updateLock = new UpdateLock();
    private final Format format;
    final RotationStrategy rotationStrategy;
    private final RotationTimerFactory rotationTimerFactory;
    private final Logger logger;
    volatile ProgressiveState<Key> state;
    private DataInitializer<EntryUpdater<Key>> stateInitializer;
    private final FileSystemAbstraction fs;
    final int keySize;
    final int valueSize;
    private volatile boolean stopped;

    public AbstractKeyValueStore( FileSystemAbstraction fs, PageCache pages, File base, RotationMonitor monitor, Logger logger,
            RotationTimerFactory timerFactory, VersionContextSupplier versionContextSupplier, int keySize,
            int valueSize, HeaderField<?>... headerFields )
    {
        this.fs = fs;
        this.keySize = keySize;
        this.valueSize = valueSize;
        Rotation rotation = getClass().getAnnotation( Rotation.class );
        if ( monitor == null )
        {
            monitor = RotationMonitor.NONE;
        }
        this.format = new Format( headerFields );
        this.logger = logger;
        this.rotationStrategy = rotation.value().create( fs, pages, format, monitor, base, rotation.parameters() );
        this.rotationTimerFactory = timerFactory;
        this.state = new DeadState.Stopped<>( format, getClass().getAnnotation( State.class ).value(),
                versionContextSupplier );
    }

    protected final void setEntryUpdaterInitializer( DataInitializer<EntryUpdater<Key>> stateInitializer )
    {
        this.stateInitializer = stateInitializer;
    }

    @Override
    public String toString()
    {
        return String.format( "%s[state=%s, hasChanges=%s]", getClass().getSimpleName(), state, state.hasChanges() );
    }

    protected final <Value> Value lookup( Key key, Reader<Value> reader ) throws IOException
    {
        ValueLookup<Value> lookup = new ValueLookup<>( reader );
        while ( true )
        {
            ProgressiveState<Key> originalState = this.state;
            try
            {
                return lookup.value( !originalState.lookup( key, lookup ) );
            }
            catch ( FileIsNotMappedException e )
            {
                // if the state has changed we think the exception is caused by a rotation event. In this
                // case we simply retry the lookup on the rotated state. Otherwise we rethrow.
                if ( originalState == this.state )
                {
                    throw e;
                }
            }
        }
    }

    /** Introspective feature, not thread safe. */
    protected final void visitAll( Visitor visitor ) throws IOException
    {
        ProgressiveState<Key> state = this.state;
        if ( visitor instanceof MetadataVisitor )
        {
            ((MetadataVisitor) visitor).visitMetadata( state.file(), headers(), state.storedEntryCount() );
        }
        try ( DataProvider provider = state.dataProvider() )
        {
            transfer( provider, visitor );
        }
    }

    protected final void visitFile( File path, Visitor visitor ) throws IOException
    {
        try ( KeyValueStoreFile file = rotationStrategy.openStoreFile( path ) )
        {
            if ( visitor instanceof MetadataVisitor )
            {
                ((MetadataVisitor) visitor).visitMetadata( path, file.headers(), file.entryCount() );
            }
            try ( DataProvider provider = file.dataProvider() )
            {
                transfer( provider, visitor );
            }
        }
    }

    protected abstract Key readKey( ReadableBuffer key ) throws UnknownKey;

    protected abstract void writeKey( Key key, WritableBuffer buffer );

    protected abstract void writeFormatSpecifier( WritableBuffer formatSpecifier );

    protected abstract Headers initialHeaders( long version );

    protected abstract int compareHeaders( Headers lhs, Headers rhs );

    protected boolean include( Key key, ReadableBuffer value )
    {
        return true;
    }

    protected final Headers headers()
    {
        return state.headers();
    }

    public int totalEntriesStored()
    {
        return state.storedEntryCount();
    }

    public final File currentFile()
    {
        return state.file();
    }

    @Override
    public final void init() throws IOException
    {
        try ( LockWrapper ignored = writeLock( updateLock, logger ) )
        {
            state = state.initialize( rotationStrategy );
        }
    }

    @Override
    public final void start() throws IOException
    {
        try ( LockWrapper ignored = writeLock( updateLock, logger ) )
        {
            state = state.start( stateInitializer );
        }
    }

    protected final Optional<EntryUpdater<Key>> updater( final long version )
    {
        try ( LockWrapper lock = readLock( updateLock, logger ) )
        {
            return state.optionalUpdater( version, lock.get() );
        }
    }

    protected final EntryUpdater<Key> updater()
    {
        try ( LockWrapper lock = readLock( updateLock, logger ) )
        {
            return state.unsafeUpdater( lock.get() );
        }
    }

    protected final EntryUpdater<Key> resetter( long version )
    {
        try ( LockWrapper lock = writeLock( updateLock, logger ) )
        {
            ProgressiveState<Key> current = state;
            return current.resetter( lock.get(), new RotationTask( version ) );
        }
    }

    /**
     * Prepare for rotation. Sets up the internal structures to ensure that all changes up to and including the changes
     * of the specified version are applied before rotation takes place. This method does not block, however if all
     * required changes have not been applied {@linkplain PreparedRotation#rotate() the rotate method} will block
     * waiting for all changes to be applied. Invoking {@linkplain PreparedRotation#rotate() the rotate method} some
     * time after all requested transactions have been applied is ok, since setting the store up for rotation does
     * not block updates, it just sorts them into updates that apply before rotation and updates that apply after.
     *
     * @param version the smallest version to include in the rotation. Note that the actual rotated version might be a
     * later version than this version. The actual rotated version is returned by
     * {@link PreparedRotation#rotate()}.
     */
    protected final PreparedRotation prepareRotation( final long version )
    {
        try ( LockWrapper ignored = writeLock( updateLock, logger ) )
        {
            ProgressiveState<Key> prior = state;
            if ( prior.storedVersion() == version && !prior.hasChanges() )
            {
                return () -> version;
            }
            return new RotationTask( version );
        }
    }

    protected abstract void updateHeaders( Headers.Builder headers, long version );

    @Override
    public final void shutdown() throws IOException
    {
        try ( LockWrapper ignored = writeLock( updateLock, logger ) )
        {
            stopped = true;
            state = state.stop();
        }
    }

    private boolean transfer( EntryVisitor<WritableBuffer> producer, EntryVisitor<ReadableBuffer> consumer )
            throws IOException
    {
        BigEndianByteArrayBuffer key = new BigEndianByteArrayBuffer( keySize );
        BigEndianByteArrayBuffer value = new BigEndianByteArrayBuffer( valueSize );
        while ( producer.visit( key, value ) )
        {
            if ( !consumer.visit( key, value ) )
            {
                return false;
            }
        }
        return true;
    }

    public Iterable<File> allFiles()
    {
        return StreamSupport.stream( rotationStrategy.candidateFiles().spliterator(), false )
                .filter( fs::fileExists )
                .collect( Collectors.toList() );
    }

    private class RotationTask implements PreparedRotation, Runnable
    {
        private final RotationState<Key> rotation;

        RotationTask( long version )
        {
            state = this.rotation = state.prepareRotation( version );
        }

        @Override
        public long rotate() throws IOException
        {
            return rotate( false );
        }

        @Override
        public void run()
        {
            try ( LockWrapper ignored = writeLock( updateLock, logger ) )
            {
                rotate( true );
            }
            catch ( IOException e )
            {
                throw new UnderlyingStorageException( e );
            }
        }

        private long rotate( boolean force ) throws IOException
        {
            try ( RotationState<Key> rotation = this.rotation )
            {
                try
                {
                    final long version = rotation.rotationVersion();
                    ProgressiveState<Key> next = rotation.rotate( force, rotationStrategy, rotationTimerFactory,
                            value -> updateHeaders( value, version ) );
                    try ( LockWrapper ignored = writeLock( updateLock, logger ) )
                    {
                        state = next;
                    }
                    return version;
                }
                catch ( Throwable t )
                {
                    // Rotation failed. Here we assume that rotation state remembers this so that closing it
                    // won't close the state as it was before rotation began, which we're reverting to right here.
                    try ( LockWrapper ignored = writeLock( updateLock, logger ) )
                    {
                        // Only mark as failed if we're still running.
                        // If shutdown has been called while being in rotation state then shutdown will fail
                        // without closing the store. This means that rotation takes over that responsibility.
                        // Therefore avoid marking rotation state as failed in this case and let the store
                        // be naturally closed before leaving this method.
                        if ( !stopped )
                        {
                            state = rotation.markAsFailed();
                        }
                    }
                    throw t;
                }
            }
        }
    }

    public abstract static class Reader<Value>
    {
        protected abstract Value parseValue( ReadableBuffer value );

        protected Value defaultValue()
        {
            return null;
        }
    }

    public abstract class Visitor implements KeyValueVisitor
    {
        @Override
        public boolean visit( ReadableBuffer key, ReadableBuffer value )
        {
            try
            {
                return visitKeyValuePair( readKey( key ), value );
            }
            catch ( UnknownKey e )
            {
                return visitUnknownKey( e, key, value );
            }
        }

        protected boolean visitUnknownKey( UnknownKey exception, ReadableBuffer key, ReadableBuffer value )
        {
            throw new IllegalArgumentException( exception.getMessage(), exception );
        }

        protected abstract boolean visitKeyValuePair( Key key, ReadableBuffer value );
    }

    private HeaderField<?>[] headerFieldsForFormat( ReadableBuffer formatSpecifier )
    {
        return format.defaultHeaderFieldsForFormat( formatSpecifier );
    }

    protected abstract long version( Headers headers );

    private final class Format extends ProgressiveFormat implements KeyFormat<Key>
    {
        Format( HeaderField<?>... headerFields )
        {
            super( 512, headerFields );
        }

        @Override
        protected void writeFormatSpecifier( WritableBuffer formatSpecifier )
        {
            AbstractKeyValueStore.this.writeFormatSpecifier( formatSpecifier );
        }

        @Override
        protected HeaderField<?>[] headerFieldsForFormat( ReadableBuffer formatSpecifier )
        {
            return AbstractKeyValueStore.this.headerFieldsForFormat( formatSpecifier );
        }

        HeaderField<?>[] defaultHeaderFieldsForFormat( ReadableBuffer formatSpecifier )
        {
            return super.headerFieldsForFormat( formatSpecifier );
        }

        @Override
        public void writeKey( Key key, WritableBuffer buffer )
        {
            AbstractKeyValueStore.this.writeKey( key, buffer );
        }

        @Override
        public int compareHeaders( Headers lhs, Headers rhs )
        {
            return AbstractKeyValueStore.this.compareHeaders( lhs, rhs );
        }

        @Override
        public Headers initialHeaders( long version )
        {
            return AbstractKeyValueStore.this.initialHeaders( version );
        }

        @Override
        public int keySize()
        {
            return AbstractKeyValueStore.this.keySize;
        }

        @Override
        public long version( Headers headers )
        {
            return AbstractKeyValueStore.this.version( headers );
        }

        @Override
        public DataProvider filter( final DataProvider provider )
        {
            return new DataProvider()
            {
                @Override
                public boolean visit( WritableBuffer key, WritableBuffer value ) throws IOException
                {
                    while ( provider.visit( key, value ) )
                    {
                        try
                        {
                            if ( include( readKey( key ), value ) )
                            {
                                return true;
                            }
                        }
                        catch ( UnknownKey e )
                        {
                            throw new IllegalArgumentException( e.getMessage(), e );
                        }
                    }
                    return false;
                }

                @Override
                public void close() throws IOException
                {
                    provider.close();
                }
            };
        }

        @Override
        public int valueSize()
        {
            return AbstractKeyValueStore.this.valueSize;
        }
    }
}
