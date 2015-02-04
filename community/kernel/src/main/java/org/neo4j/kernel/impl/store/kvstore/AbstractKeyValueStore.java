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

import static org.neo4j.kernel.impl.locking.LockWrapper.readLock;
import static org.neo4j.kernel.impl.locking.LockWrapper.writeLock;

/**
 * The base for building a key value store based on rotating immutable
 * {@linkplain KeyValueStoreFile key/value store files}
 *
 * @param <Key>      a base type for the keys stored in this store.
 * @param <MetaData> a type for containing the metadata of the store.
 * @param <MetaDiff> a type that signifies changes in metadata on rotation.
 */
@Rotation(/*default strategy:*/Rotation.Strategy.LEFT_RIGHT/*(subclasses can override)*/)
@State(/*default strategy:*/State.Strategy.CONCURRENT_HASH_MAP/*(subclasses can override)*/)
public abstract class AbstractKeyValueStore<Key, MetaData, MetaDiff> extends LifecycleAdapter
{
    private final ReadWriteLock updateLock = new ReentrantReadWriteLock( /*fair=*/true );
    private volatile KeyValueStoreState<Key, MetaData> state;
    final int keySize, valueSize;

    public AbstractKeyValueStore( FileSystemAbstraction fs, PageCache pages, File base, int keySize, int valueSize,
                                  HeaderField<MetaData, ?>... headerFields )
    {
        this.keySize = keySize;
        this.valueSize = valueSize;
        Rotation rotation = getClass().getAnnotation( Rotation.class );
        Format format = new Format( headerFields );
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
        try ( DataProvider provider = state.dataProvider() )
        {
            transfer( provider, visitor );
        }
    }

    protected abstract Key readKey( ReadableBuffer key );

    protected abstract void writeKey( Key key, WritableBuffer buffer );

    protected abstract void writeFormatSpecifier( WritableBuffer formatSpecifier );

    protected abstract MetaData initialMetadata();

    protected abstract int compareMetadata( MetaData lhs, MetaData rhs );

    protected boolean hasMetadataChanges( MetaData metadata, MetaDiff diff )
    {
        return true;
    }

    protected abstract MetaData buildMetadata( ReadableBuffer formatSpecifier, CollectedMetadata metadata );

    protected abstract MetaData updateMetadata( MetaData metadata, MetaDiff changes );

    protected abstract String extractFileTrailer( MetaData metadata );

    protected boolean include( Key key, ReadableBuffer value )
    {
        return true;
    }

    protected final MetaData metadata()
    {
        return state.metadata();
    }

    public int totalRecordsStored()
    {
        return state.totalRecordsStored();
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

    protected final void apply( Update<Key> update ) throws IOException
    {
        try ( LockWrapper ignored = readLock( updateLock ) )
        {
            state.apply( update );
        }
    }

    public void visitFile( AbstractKeyValueVisitor<Key, MetaData> visitor ) throws IOException
    {
        KeyValueStoreState<Key, MetaData> state = this.state;
        visitor.visitMetadata( state.file(), metadata(), state.totalRecordsStored() );
        try ( DataProvider provider = state.dataProvider() )
        {
            transfer( provider, new DelegatingKeyValueVisitor( visitor ) );
        }
    }

    public void visitFile( File path, AbstractKeyValueVisitor<Key, MetaData> visitor ) throws IOException
    {
        try ( KeyValueStoreFile<MetaData> file = state.openStoreFile( path ) )
        {
            visitor.visitMetadata( path, file.metadata(), file.recordCount() );
            try ( DataProvider provider = file.dataProvider() )
            {
                transfer( provider, new DelegatingKeyValueVisitor( visitor ) );
            }
        }
    }

    protected void failedToOpenStoreFile( File path, Exception error )
    {
        // override to implement logging
    }

    protected void rotationFailed( File source, File target, MetaData metaData, Exception e )
    {
        // override to implement logging
    }

    protected void rotationSucceeded( File source, File target, MetaData metaData )
    {
        // override to implement logging
    }

    protected void beforeRotation( File source, File target, MetaData metaData )
    {
        // override to implement logging
    }

    protected abstract class Updater implements AutoCloseable
    {
        private final KeyValueStoreState<Key, MetaData> state;
        private Thread thread = Thread.currentThread();

        public Updater()
        {
            updateLock.readLock().lock();
            this.state = AbstractKeyValueStore.this.state;
        }

        protected final void apply( Update<Key> update ) throws IOException
        {
            if ( thread != Thread.currentThread() )
            {
                throw new IllegalStateException( "Updater of " + AbstractKeyValueStore.this + " is not available." );
            }
            state.apply( update );
        }

        @Override
        public final void close()
        {
            thread = null;
            updateLock.readLock().unlock();
        }
    }

    protected final void rotate( MetaDiff metadataChanges ) throws IOException
    {
        try ( LockWrapper ignored = writeLock( updateLock ) )
        {
            KeyValueStoreState<Key, MetaData> current = state;
            if ( !current.hasChanges() )
            {
                if ( !hasMetadataChanges( current.metadata(), metadataChanges ) )
                {
                    return;
                }
            }
            state = state.rotate( updateMetadata( current.metadata(), metadataChanges ) );
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

    public static abstract class Update<Key>
    {
        final Key key;

        public Update( Key key )
        {
            this.key = key;
        }

        protected abstract void update( WritableBuffer value );
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
            return visitKeyValuePair( readKey( key ), value );
        }

        protected abstract boolean visitKeyValuePair( Key key, ReadableBuffer value );
    }

    private class DelegatingKeyValueVisitor extends Visitor
    {
        private final AbstractKeyValueVisitor<Key, MetaData> visitor;

        DelegatingKeyValueVisitor( AbstractKeyValueVisitor<Key, MetaData> visitor )
        {
            this.visitor = visitor;
        }

        @Override
        protected boolean visitKeyValuePair( Key key, ReadableBuffer value )
        {
            visitor.visitData( key, value );
            return true;
        }
    }

    private final class Format extends ProgressiveFormat<MetaData> implements KeyFormat<Key>
    {
        Format( HeaderField<MetaData, ?>... headerFields )
        {
            super( 512, headerFields );
        }

        @Override
        protected void writeFormatSpecifier( WritableBuffer formatSpecifier )
        {
            AbstractKeyValueStore.this.writeFormatSpecifier( formatSpecifier );
        }

        @Override
        protected String extractFileTrailer( MetaData metadata )
        {
            return AbstractKeyValueStore.this.extractFileTrailer( metadata );
        }

        @Override
        protected MetaData buildMetadata( ReadableBuffer formatSpecifier, CollectedMetadata metadata )
        {
            return AbstractKeyValueStore.this.buildMetadata( formatSpecifier, metadata );
        }

        @Override
        public void writeKey( Key key, WritableBuffer buffer )
        {
            AbstractKeyValueStore.this.writeKey( key, buffer );
        }

        @Override
        public int compareMetadata( MetaData lhs, MetaData rhs )
        {
            return AbstractKeyValueStore.this.compareMetadata( lhs, rhs );
        }

        @Override
        public MetaData initialMetadata()
        {
            return AbstractKeyValueStore.this.initialMetadata();
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
                        if ( include( readKey( key ), value ) )
                        {
                            return true;
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
        public void beforeRotation( File source, File target, MetaData metaData )
        {
            AbstractKeyValueStore.this.beforeRotation( source, target, metaData );
        }

        @Override
        public void rotationSucceeded( File source, File target, MetaData metaData )
        {
            AbstractKeyValueStore.this.rotationSucceeded( source, target, metaData );
        }

        @Override
        public void rotationFailed( File source, File target, MetaData metaData, Exception e )
        {
            AbstractKeyValueStore.this.rotationFailed( source, target, metaData, e );
        }
    }
}
