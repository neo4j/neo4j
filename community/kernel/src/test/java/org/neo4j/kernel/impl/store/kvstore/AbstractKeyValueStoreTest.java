/*
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
import java.nio.ByteBuffer;
import java.util.concurrent.Future;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.function.IOFunction;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Predicate;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.test.ThreadingRule;

import static java.util.concurrent.TimeUnit.SECONDS;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import static org.neo4j.kernel.impl.store.kvstore.DataProvider.EMPTY_DATA_PROVIDER;
import static org.neo4j.kernel.impl.store.kvstore.Resources.InitialLifecycle.STARTED;
import static org.neo4j.kernel.impl.store.kvstore.Resources.TestPath.FILE_IN_EXISTING_DIRECTORY;

public class AbstractKeyValueStoreTest
{
    public final @Rule Resources the = new Resources( FILE_IN_EXISTING_DIRECTORY );
    public final @Rule ThreadingRule threading = new ThreadingRule();
    private static final HeaderField<Long> TX_ID = new HeaderField<Long>()
    {
        @Override
        public Long read( ReadableBuffer header )
        {
            return header.getLong( header.size() - 8 );
        }

        @Override
        public void write( Long value, WritableBuffer header )
        {
            header.putLong( header.size() - 8, value );
        }

        @Override
        public String toString()
        {
            return "txId";
        }
    };

    @Test
    public void shouldStartAndStopStore() throws Exception
    {
        // given
        the.managed( new Store() );

        // when
        the.lifeStarts();
        the.lifeShutsDown();
    }

    @Test
    @Resources.Life(STARTED)
    public void shouldRotateStore() throws Exception
    {
        // given
        Store store = the.managed( new Store() );

        // when
        store.prepareRotation( 0 ).rotate();
    }

    @Test
    @Resources.Life(STARTED)
    public void shouldStoreEntries() throws Exception
    {
        // given
        Store store = the.managed( new Store() );

        // when
        store.put( "message", "hello world" );
        store.put( "age", "too old" );

        // then
        assertEquals( "hello world", store.get( "message" ) );
        assertEquals( "too old", store.get( "age" ) );

        // when
        store.prepareRotation( 0 ).rotate();

        // then
        assertEquals( "hello world", store.get( "message" ) );
        assertEquals( "too old", store.get( "age" ) );
    }

    @Test
    public void shouldPickFileWithGreatestTransactionId() throws Exception
    {
        // given
        class Impl extends Store
        {
            Impl()
            {
                super( TX_ID );
            }

            @SuppressWarnings("unchecked")
            @Override
            <Value> Value initialHeader( HeaderField<Value> field )
            {
                if ( field == TX_ID )
                {
                    return (Value) (Object) 1l;
                }
                else
                {
                    return super.initialHeader( field );
                }
            }

            @Override
            protected int compareHeaders( Headers lhs, Headers rhs )
            {
                return Long.compare( lhs.get( TX_ID ), rhs.get( TX_ID ) );
            }

            @Override
            protected long version( Headers headers )
            {
                return headers.get( TX_ID );
            }

            @Override
            protected void updateHeaders( Headers.Builder headers, long version )
            {
                headers.put( TX_ID, version );
            }
        }
        try ( Lifespan life = new Lifespan() )
        {
            Store store = life.add( new Impl() );

            // when
            for ( long txId = 2; txId <= 10; txId++ )
            {
                store.updater( txId ).get().close();
                store.prepareRotation( txId ).rotate();
            }
        }

        // then
        try ( Lifespan life = new Lifespan() )
        {
            Store store = life.add( new Impl() );
            assertEquals( 10l, store.headers().get( TX_ID ).longValue() );
        }
    }

    @Test
    public void shouldNotPickCorruptStoreFile() throws Exception
    {
        // given
        Store store = new Store( TX_ID )
        {
            @SuppressWarnings("unchecked")
            @Override
            <Value> Value initialHeader( HeaderField<Value> field )
            {
                if ( field == TX_ID )
                {
                    return (Value) (Object) 1l;
                }
                else
                {
                    return super.initialHeader( field );
                }
            }

            @Override
            protected int compareHeaders( Headers lhs, Headers rhs )
            {
                return Long.compare( lhs.get( TX_ID ), rhs.get( TX_ID ) );
            }
        };
        RotationStrategy rotation = store.rotationStrategy;

        // when
        File[] files = new File[10];
        {
            Pair<File, KeyValueStoreFile> file = rotation.create( EMPTY_DATA_PROVIDER, 1 );
            files[0] = file.first();
            for ( int txId = 2, i = 1; i < files.length; txId <<= 1, i++ )
            {
                KeyValueStoreFile old = file.other();
                final int data = txId;
                file = rotation.next( file.first(), Headers.headersBuilder().put( TX_ID, (long) txId ).headers(), data(
                        new Entry()
                        {
                            @Override
                            public void write( WritableBuffer key, WritableBuffer value )
                            {
                                key.putByte( 0, (byte) 'f' );
                                key.putByte( 1, (byte) 'o' );
                                key.putByte( 2, (byte) 'o' );
                                value.putInt( 0, data );
                            }
                        }
                ) );
                old.close();
                files[i] = file.first();
            }
            file.other().close();
        }
        // Corrupt the last files
        try ( StoreChannel channel = the.fileSystem().open( files[9], "rw" ) )
        {   // ruin the header
            channel.position( 16 );
            ByteBuffer value = ByteBuffer.allocate( 16 );
            value.put( (byte) 0 );
            value.flip();
            channel.writeAll( value );
        }
        try ( StoreChannel channel = the.fileSystem().open( files[8], "rw" ) )
        {   // ruin the header
            channel.position( 32 );
            ByteBuffer value = ByteBuffer.allocate( 16 );
            value.put( (byte) 17 );
            value.flip();
            channel.writeAll( value );
        }
        try ( StoreChannel channel = the.fileSystem().open( files[7], "rw" ) )
        {   // ruin the header
            channel.position( 32 + 32 + 32 + 16 );
            ByteBuffer value = ByteBuffer.allocate( 16 );
            value.putLong( 0 );
            value.putLong( 0 );
            value.flip();
            channel.writeAll( value );
        }

        // then
        try ( Lifespan life = new Lifespan() )
        {
            life.add( store );

            assertEquals( 64l, store.headers().get( TX_ID ).longValue() );
        }
    }

    @Test
    @Resources.Life(STARTED)
    public void shouldRotateWithCorrectVersion() throws Exception
    {
        // given
        final Store store = the.managed( new Store( TX_ID )
        {
            @SuppressWarnings("unchecked")
            @Override
            <Value> Value initialHeader( HeaderField<Value> field )
            {
                if ( field == TX_ID )
                {
                    return (Value) (Object) 1l;
                }
                else
                {
                    return super.initialHeader( field );
                }
            }

            @Override
            protected void updateHeaders( Headers.Builder headers, long version )
            {
                headers.put( TX_ID, version );
            }

            @Override
            protected int compareHeaders( Headers lhs, Headers rhs )
            {
                return Long.compare( lhs.get( TX_ID ), rhs.get( TX_ID ) );
            }
        } );
        IOFunction<Long, Void> update = new IOFunction<Long, Void>()
        {
            @Override
            public Void apply( Long update ) throws IOException
            {
                try ( EntryUpdater<String> updater = store.updater( update ).get() )
                {
                    updater.apply( "key " + update, store.value( "value " + update ) );
                }
                return null;
            }
        };

        // when
        update.apply( 1l );
        PreparedRotation rotation = store.prepareRotation( 2 );
        update.apply( 2l );
        rotation.rotate();

        // then
        assertEquals( 2, store.headers().get( TX_ID ).longValue() );
        store.prepareRotation( 2 ).rotate();
    }

    @Test
    @Resources.Life(STARTED)
    public void shouldBlockRotationUntilRequestedTransactionsAreApplied() throws Exception
    {
        // given
        final Store store = the.managed( new Store( TX_ID )
        {
            @SuppressWarnings("unchecked")
            @Override
            <Value> Value initialHeader( HeaderField<Value> field )
            {
                if ( field == TX_ID )
                {
                    return (Value) (Object) 1l;
                }
                else
                {
                    return super.initialHeader( field );
                }
            }

            @Override
            protected void updateHeaders( Headers.Builder headers, long version )
            {
                headers.put( TX_ID, version );
            }

            @Override
            protected int compareHeaders( Headers lhs, Headers rhs )
            {
                return Long.compare( lhs.get( TX_ID ), rhs.get( TX_ID ) );
            }
        } );
        IOFunction<Long, Void> update = new IOFunction<Long, Void>()
        {
            @Override
            public Void apply( Long update ) throws IOException
            {
                try ( EntryUpdater<String> updater = store.updater( update ).get() )
                {
                    updater.apply( "key " + update, store.value( "value " + update ) );
                }
                return null;
            }
        };

        // when
        update.apply( 1l );
        Future<Long> rotation = threading.executeAndAwait( store.rotation, 3l, new Predicate<Thread>()
        {
            @Override
            public boolean accept( Thread thread )
            {
                switch ( thread.getState() )
                {
                case BLOCKED:
                case WAITING:
                case TIMED_WAITING:
                case TERMINATED:
                    return true;
                default:
                    return false;
                }
            }
        }, 100, SECONDS );
        // rotation should wait...
        assertFalse( rotation.isDone() );
        SECONDS.sleep( 1 );
        assertFalse( rotation.isDone() );
        // apply update
        update.apply( 3l );
        // rotation should still wait...
        assertFalse( rotation.isDone() );
        SECONDS.sleep( 1 );
        assertFalse( rotation.isDone() );
        // apply update
        update.apply( 4l );
        // rotation should still wait...
        assertFalse( rotation.isDone() );
        SECONDS.sleep( 1 );
        assertFalse( rotation.isDone() );
        // apply update
        update.apply( 2l );

        // then
        assertEquals( 3, rotation.get().longValue() );
        assertEquals( 3, store.headers().get( TX_ID ).longValue() );
        store.rotation.apply( 4l );
    }

    static DataProvider data( final Entry... data )
    {
        return new DataProvider()
        {
            int i;

            @Override
            public boolean visit( WritableBuffer key, WritableBuffer value ) throws IOException
            {
                if ( i < data.length )
                {
                    data[i++].write( key, value );
                    return true;
                }
                return false;
            }

            @Override
            public void close() throws IOException
            {
            }
        };
    }

    interface Entry
    {
        void write( WritableBuffer key, WritableBuffer value );
    }

    @Rotation(Rotation.Strategy.INCREMENTING)
    class Store extends AbstractKeyValueStore<String>
    {
        private final HeaderField<?>[] headerFields;
        final IOFunction<Long, Long> rotation = new IOFunction<Long, Long>()
        {
            @Override
            public Long apply( Long version ) throws IOException
            {
                return prepareRotation( version ).rotate();
            }
        };

        private Store( HeaderField<?>... headerFields )
        {
            super( the.fileSystem(), the.pageCache(), the.testPath(), null, 16, 16, headerFields );
            this.headerFields = headerFields;
            setEntryUpdaterInitializer( new DataInitializer<EntryUpdater<String>>()
            {
                @Override
                public void initialize( EntryUpdater<String> stringEntryUpdater )
                {
                }

                @Override
                public long initialVersion()
                {
                    return 0;
                }
            } );
        }

        @Override
        protected Headers initialHeaders( long version )
        {
            Headers.Builder builder = Headers.headersBuilder();
            for ( HeaderField<?> field : headerFields )
            {
                putHeader( builder, field );
            }
            return builder.headers();
        }

        private <Value> void putHeader( Headers.Builder builder, HeaderField<Value> field )
        {
            builder.put( field, initialHeader( field ) );
        }

        <Value> Value initialHeader( HeaderField<Value> field )
        {
            return null;
        }

        @Override
        protected int compareHeaders( Headers lhs, Headers rhs )
        {
            return 0;
        }

        @SuppressWarnings("unchecked")
        private <Value> void putField( Headers.Builder builder, HeaderField<Value> field, Object change )
        {
            builder.put( field, (Value) change );
        }

        @Override
        protected void writeKey( String key, WritableBuffer buffer )
        {
            for ( int i = 0; i < key.length(); i++ )
            {
                char c = key.charAt( i );
                if ( c == 0 || c >= 128 )
                {
                    throw new IllegalArgumentException( "Only ASCII keys allowed." );
                }
                buffer.putByte( i, (byte) c );
            }
        }

        @Override
        protected String readKey( ReadableBuffer key )
        {
            StringBuilder result = new StringBuilder( 16 );
            for ( int i = 0; i < key.size(); i++ )
            {
                char c = (char) (0xFF & key.getByte( i ));
                if ( c == 0 )
                {
                    break;
                }
                result.append( c );
            }
            return result.toString();
        }

        @Override
        protected String fileTrailer()
        {
            return "And that's all folks.";
        }

        @Override
        protected void updateHeaders( Headers.Builder headers, long version )
        {
        }

        @Override
        protected long version( Headers headers )
        {
            try
            {
                String filename = this.currentFile().getName();
                return Integer.parseInt( filename.substring( filename.lastIndexOf( '.' ) + 1 ) );
            }
            catch ( IllegalStateException e )
            {
                return 0;
            }
        }

        @Override
        protected void writeFormatSpecifier( WritableBuffer formatSpecifier )
        {
            formatSpecifier.putByte( 0, (byte) 0xFF );
            formatSpecifier.putByte( formatSpecifier.size() - 1, (byte) 0xFF );
        }

        public void put( String key, final String value ) throws IOException
        {
            try ( EntryUpdater<String> updater = updater() )
            {
                updater.apply( key, value( value ) );
            }
        }

        ValueUpdate value( final String value )
        {
            return new ValueUpdate()
            {
                @Override
                public void update( WritableBuffer target )
                {
                    writeKey( value, target );
                }
            };
        }

        public String get( String key ) throws IOException
        {
            return lookup( key, new Reader<String>()
            {
                @Override
                protected String parseValue( ReadableBuffer value )
                {
                    return readKey( value );
                }
            } );
        }
    }
}