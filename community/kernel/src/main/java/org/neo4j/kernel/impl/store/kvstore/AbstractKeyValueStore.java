/**
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
package org.neo4j.kernel.impl.store.kvstore;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.locking.LockWrapper;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static org.neo4j.kernel.impl.locking.LockWrapper.writeLock;

/**
 * The base for building a key value store based on rotating immutable
 * {@linkplain KeyValueStoreFile key/value store files}
 *
 * @param <Key>           a base type for the keys stored in this store.
 * @param <HeaderChanges> a type that signifies changes in headers on rotation.
 */
@Rotation(/*default strategy:*/Rotation.Strategy.LEFT_RIGHT/*(subclasses can override)*/)
@State(/*default strategy:*/State.Strategy.CONCURRENT_HASH_MAP/*(subclasses can override)*/)
public abstract class AbstractKeyValueStore<Key, HeaderChanges> extends LifecycleAdapter
{
    private final ReadWriteLock updateLock = new ReentrantReadWriteLock( /*fair=*/true );
    private final Format format;
    private volatile KeyValueStoreState<Key> state;
    final int keySize, valueSize;

    public AbstractKeyValueStore( FileSystemAbstraction fs, PageCache pages, File base, int keySize, int valueSize,
                                  HeaderField<?>... headerFields )
    {
        this.keySize = keySize;
        this.valueSize = valueSize;
        Rotation rotation = getClass().getAnnotation( Rotation.class );
        this.format = new Format( headerFields );
        this.state = getClass().getAnnotation( State.class ).value().initialState(
                rotation.value().create( fs, pages, format, base, rotation.parameters() ), format );
    }

    @Override
    public String toString()
    {
        return String.format( "%s[state=%s, hasChanges=%s]", getClass().getSimpleName(), state, state.hasChanges() );
    }

    protected final <Value> Value lookup( Key key, Reader<Value> lookup ) throws IOException
    {
        return state.lookup( key, lookup );
    }

    /** Introspective feature, not thread safe. */
    protected final void visitAll( Visitor visitor ) throws IOException
    {
        KeyValueStoreState<Key> state = this.state;
        if ( visitor instanceof MetadataVisitor )
        {
            ((MetadataVisitor) visitor).visitMetadata( state.file(), headers(), state.totalEntriesStored() );
        }
        try ( DataProvider provider = state.dataProvider() )
        {
            transfer( provider, visitor );
        }
    }

    protected final void visitFile( File path, Visitor visitor ) throws IOException
    {
        try ( KeyValueStoreFile file = state.openStoreFile( path ) )
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

    protected abstract Headers initialHeaders();

    protected abstract int compareHeaders( Headers lhs, Headers rhs );

    protected boolean hasHeaderChanges( Headers headers, HeaderChanges diff )
    {
        return true;
    }

    protected abstract Headers updateHeaders( Headers headers, HeaderChanges changes );

    protected abstract String fileTrailer();

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
        return state.totalEntriesStored();
    }

    public final File currentFile()
    {
        return state.file();
    }

    @Override
    public final void init() throws IOException
    {
        try ( LockWrapper ignored = writeLock( updateLock ) )
        {
            state = state.init();
        }
    }

    @Override
    public final void start() throws IOException
    {
        try ( LockWrapper ignored = writeLock( updateLock ) )
        {
            state = state.start();
        }
    }

    protected void failedToOpenStoreFile( File path, Exception error )
    {
        // override to implement logging
    }

    protected void rotationFailed( File source, File target, Headers headers, Exception e )
    {
        // override to implement logging
    }

    protected void rotationSucceeded( File source, File target, Headers headers )
    {
        // override to implement logging
    }

    protected void beforeRotation( File source, File target, Headers headers )
    {
        // override to implement logging
    }

    protected final EntryUpdater<Key> updater( long txId )
    {
        return updater(); // TODO: this method should care about the txId
    }

    protected final EntryUpdater<Key> updater()
    {
        return new EntryUpdater<Key>( updateLock.readLock() )
        {
            private final KeyValueStoreState<Key> state = AbstractKeyValueStore.this.state;

            @Override
            public void apply( Key key, ValueUpdate update ) throws IOException
            {
                ensureSameThread();
                state.apply( key, update, false );
            }
        };
    }

    protected final EntryUpdater<Key> resetter()
    {
        return new EntryUpdater<Key>( updateLock.writeLock() )
        {
            private final KeyValueStoreState<Key> state = AbstractKeyValueStore.this.state;

            {
                if ( state.hasChanges() )
                {
                    close();
                    throw new IllegalStateException( "Cannot reset when there are changes!" );
                }
            }

            @Override
            public void apply( Key key, ValueUpdate update ) throws IOException
            {
                ensureOpen();
                state.apply( key, update, true );
            }
        };
    }

    protected final void rotate( HeaderChanges headerChanges ) throws IOException
    {
        try ( LockWrapper ignored = writeLock( updateLock ) )
        {
            KeyValueStoreState<Key> current = state;
            if ( !current.hasChanges() )
            {
                if ( !hasHeaderChanges( current.headers(), headerChanges ) )
                {
                    return;
                }
            }
            state = state.rotate( updateHeaders( current.headers(), headerChanges ) );
        }
    }

    @Override
    public final void shutdown() throws IOException
    {
        state = state.shutdown();
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

    public static abstract class Reader<Value>
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

    protected HeaderField<?>[] headerFieldsForFormat( ReadableBuffer formatSpecifier )
    {
        return format.defaultHeaderFieldsForFormat( formatSpecifier );
    }

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
        protected String fileTrailer()
        {
            return AbstractKeyValueStore.this.fileTrailer();
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
        public Headers initialHeaders()
        {
            return AbstractKeyValueStore.this.initialHeaders();
        }

        @Override
        public int keySize()
        {
            return AbstractKeyValueStore.this.keySize;
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

        @Override
        public void failedToOpenStoreFile( File path, Exception error )
        {
            AbstractKeyValueStore.this.failedToOpenStoreFile( path, error );
        }

        @Override
        public void beforeRotation( File source, File target, Headers headers )
        {
            AbstractKeyValueStore.this.beforeRotation( source, target, headers );
        }

        @Override
        public void rotationSucceeded( File source, File target, Headers headers )
        {
            AbstractKeyValueStore.this.rotationSucceeded( source, target, headers );
        }

        @Override
        public void rotationFailed( File source, File target, Headers headers, Exception e )
        {
            AbstractKeyValueStore.this.rotationFailed( source, target, headers, e );
        }
    }
}
