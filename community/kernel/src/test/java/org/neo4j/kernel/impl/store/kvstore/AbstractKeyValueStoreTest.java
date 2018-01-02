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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.function.IOFunction;
import org.neo4j.function.Predicate;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.helpers.Clock;
import org.neo4j.helpers.Pair;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.test.ThreadingRule;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.neo4j.kernel.impl.store.kvstore.DataProvider.EMPTY_DATA_PROVIDER;
import static org.neo4j.kernel.impl.store.kvstore.Resources.InitialLifecycle.STARTED;
import static org.neo4j.kernel.impl.store.kvstore.Resources.TestPath.FILE_IN_EXISTING_DIRECTORY;

public class AbstractKeyValueStoreTest
{
    @Rule
    public final Resources resourceManager = new Resources( FILE_IN_EXISTING_DIRECTORY );
    @Rule
    public final ThreadingRule threading = new ThreadingRule();
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

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
    @Resources.Life( STARTED )
    public void retryLookupOnConcurrentStoreStateChange() throws IOException
    {
        Store testStore = resourceManager.managed( createTestStore( TimeUnit.DAYS.toMillis( 2 ) ) );
        ConcurrentMapState<String> newState = new ConcurrentMapState<>( testStore.state, mock( File.class ) );
        testStore.put( "test", "value" );

        CountingErroneousReader countingErroneousReader = new CountingErroneousReader( testStore, newState );

        assertEquals( "New state contains stored value", "value", testStore.lookup( "test", countingErroneousReader ) );
        assertEquals( "Should have 2 invocations: first throws exception, second re-read value.", 2,
                countingErroneousReader.getInvocationCounter() );
    }

    @Test
    @Resources.Life( STARTED )
    public void accessClosedStateCauseIllegalStateException() throws Exception
    {
        Store store = resourceManager.managed( new Store() );
        store.put( "test", "value" );
        store.prepareRotation( 0 ).rotate();
        ProgressiveState<String> lookupState = store.state;
        store.prepareRotation( 0 ).rotate();

        expectedException.expect( IllegalStateException.class );
        expectedException.expectMessage( "File has been unmapped" );

        lookupState.lookup( "test", new ValueSink()
        {
            @Override
            protected void value( ReadableBuffer value )
            {
                // empty
            }
        } );
    }

    @Test
    public void shouldStartAndStopStore() throws Exception
    {
        // given
        resourceManager.managed( new Store() );

        // when
        resourceManager.lifeStarts();
        resourceManager.lifeShutsDown();
    }

    @Test
    @Resources.Life( STARTED )
    public void shouldRotateStore() throws Exception
    {
        // given
        Store store = resourceManager.managed( new Store() );

        // when
        store.prepareRotation( 0 ).rotate();
    }

    @Test
    @Resources.Life( STARTED )
    public void shouldStoreEntries() throws Exception
    {
        // given
        Store store = resourceManager.managed( new Store() );

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
        try ( Lifespan life = new Lifespan() )
        {
            Store store = life.add( createTestStore() );

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
            Store store = life.add( createTestStore() );
            assertEquals( 10L, store.headers().get( TX_ID ).longValue() );
        }
    }

    @Test
    public void shouldNotPickCorruptStoreFile() throws Exception
    {
        // given
        Store store = createTestStore();
        RotationStrategy rotation = store.rotationStrategy;

        // when
        File[] files = new File[10];
        {
            Pair<File,KeyValueStoreFile> file = rotation.create( EMPTY_DATA_PROVIDER, 1 );
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
        try ( StoreChannel channel = resourceManager.fileSystem().open( files[9], "rw" ) )
        {   // ruin the header
            channel.position( 16 );
            ByteBuffer value = ByteBuffer.allocate( 16 );
            value.put( (byte) 0 );
            value.flip();
            channel.writeAll( value );
        }
        try ( StoreChannel channel = resourceManager.fileSystem().open( files[8], "rw" ) )
        {   // ruin the header
            channel.position( 32 );
            ByteBuffer value = ByteBuffer.allocate( 16 );
            value.put( (byte) 17 );
            value.flip();
            channel.writeAll( value );
        }
        try ( StoreChannel channel = resourceManager.fileSystem().open( files[7], "rw" ) )
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

            assertEquals( 64L, store.headers().get( TX_ID ).longValue() );
        }
    }

    @Test
    public void shouldPickTheUncorruptedStoreWhenTruncatingAfterTheHeader() throws IOException
    {
        /*
         * The problem was that if we were succesfull in writing the header but failing immediately after, we would
         *  read 0 as counter for entry data and pick the corrupted store thinking that it was simply empty.
         */

        Store store = createTestStore();

        Pair<File,KeyValueStoreFile> file = store.rotationStrategy.create( EMPTY_DATA_PROVIDER, 1 );
        Pair<File,KeyValueStoreFile> next = store.rotationStrategy
                .next( file.first(), Headers.headersBuilder().put( TX_ID, (long) 42 ).headers(), data( new Entry()
                {
                    @Override
                    public void write( WritableBuffer key, WritableBuffer value )
                    {
                        key.putByte( 0, (byte) 'f' );
                        key.putByte( 1, (byte) 'o' );
                        key.putByte( 2, (byte) 'o' );
                        value.putInt( 0, 42 );
                    }
                } ) );
        file.other().close();
        File correct = next.first();

        Pair<File,KeyValueStoreFile> nextNext = store.rotationStrategy
                .next( correct, Headers.headersBuilder().put( TX_ID, (long) 43 ).headers(), data( new Entry()
                {
                    @Override
                    public void write( WritableBuffer key, WritableBuffer value )
                    {
                        key.putByte( 0, (byte) 'f' );
                        key.putByte( 1, (byte) 'o' );
                        key.putByte( 2, (byte) 'o' );
                        value.putInt( 0, 42 );
                    }
                }, new Entry()
                {
                    @Override
                    public void write( WritableBuffer key, WritableBuffer value )
                    {
                        key.putByte( 0, (byte) 'b' );
                        key.putByte( 1, (byte) 'a' );
                        key.putByte( 2, (byte) 'r' );
                        value.putInt( 0, 4242 );
                    }
                }) );
        next.other().close();
        File corrupted = nextNext.first();
        nextNext.other().close();

        try ( StoreChannel channel = resourceManager.fileSystem().open( corrupted, "rw" ) )
        {
            channel.truncate( 16*4 );
        }

        // then
        try ( Lifespan life = new Lifespan() )
        {
            life.add( store );

            assertNotNull( store.get( "foo" ) );
            assertEquals( 42L, store.headers().get( TX_ID ).longValue() );
        }
    }

    @Test
    @Resources.Life( STARTED )
    public void shouldRotateWithCorrectVersion() throws Exception
    {
        // given
        final Store store = resourceManager.managed( createTestStore() );
        updateStore( store, 1 );

        PreparedRotation rotation = store.prepareRotation( 2 );
        updateStore( store, 2 );
        rotation.rotate();

        // then
        assertEquals( 2, store.headers().get( TX_ID ).longValue() );
        store.prepareRotation( 2 ).rotate();
    }

    @Test
    @Resources.Life( STARTED )
    public void postStateUpdatesCountedOnlyForTransactionsGreaterThanRotationVersion()
            throws IOException, TimeoutException, InterruptedException, ExecutionException
    {
        final Store store = resourceManager.managed( createTestStore() );

        PreparedRotation rotation = store.prepareRotation( 2 );
        updateStore( store, 4 );
        updateStore( store, 3 );
        updateStore( store, 1 );
        updateStore( store, 2 );

        assertEquals( 2, rotation.rotate() );

        Future<Long> rotationFuture = threading.executeAndAwait( store.rotation, 5L, new Predicate<Thread>()
        {
            @Override
            public boolean test( Thread thread )
            {
                return Thread.State.TIMED_WAITING == thread.getState();
            }
        }, 100, SECONDS );

        Thread.sleep( TimeUnit.SECONDS.toMillis( 1 ) );

        assertFalse( rotationFuture.isDone() );
        updateStore( store, 5 );

        assertEquals( 5, rotationFuture.get().longValue() );
    }

    @Test
    @Resources.Life( STARTED )
    public void shouldBlockRotationUntilRequestedTransactionsAreApplied() throws Exception
    {
        // given
        final Store store = resourceManager.managed( createTestStore() );

        // when
        updateStore( store, 1 );
        Future<Long> rotation = threading.executeAndAwait( store.rotation, 3L, new Predicate<Thread>()
        {
            @Override
            public boolean test( Thread thread )
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
        updateStore( store, 3 );
        // rotation should still wait...
        assertFalse( rotation.isDone() );
        SECONDS.sleep( 1 );
        assertFalse( rotation.isDone() );
        // apply update
        updateStore( store, 4 );
        // rotation should still wait...
        assertFalse( rotation.isDone() );
        SECONDS.sleep( 1 );
        assertFalse( rotation.isDone() );
        // apply update
        updateStore( store, 2 );

        // then
        assertEquals( 3, rotation.get().longValue() );
        assertEquals( 3, store.headers().get( TX_ID ).longValue() );
        store.rotation.apply( 4L );
    }

    @Test( timeout = 2000 )
    @Resources.Life( STARTED )
    public void shouldFailRotationAfterTimeout() throws IOException
    {

        // GIVEN
        final Store store = resourceManager.managed( createTestStore( 0 ) );

        // THEN
        expectedException.expect( RotationTimeoutException.class );

        // WHEN
        store.prepareRotation( 10L ).rotate();
    }

    private Store createTestStore()
    {
        return createTestStore( TimeUnit.SECONDS.toMillis( 100 ) );
    }

    private Store createTestStore( long rotationTimeout )
    {
        return new Store( rotationTimeout, TX_ID )
        {
            @SuppressWarnings( "unchecked" )
            @Override
            <Value> Value initialHeader( HeaderField<Value> field )
            {
                if ( field == TX_ID )
                {
                    return (Value) (Object) 1L;
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
        };
    }

    private void updateStore( final Store store, long transaction ) throws IOException
    {
        ThrowingConsumer<Long,IOException> update = new ThrowingConsumer<Long,IOException>()
        {
            @Override
            public void accept( Long update ) throws IOException
            {
                try ( EntryUpdater<String> updater = store.updater( update ).get() )
                {
                    updater.apply( "key " + update, store.value( "value " + update ) );
                }
            }
        };
        update.accept( transaction );
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

    private static class CountingErroneousReader extends AbstractKeyValueStore.Reader<String>
    {
        private final Store testStore;
        private final ProgressiveState<String> newStoreState;
        private int invocationCounter;

        CountingErroneousReader( Store testStore, ProgressiveState<String> newStoreState )
        {
            this.testStore = testStore;
            this.newStoreState = newStoreState;
            invocationCounter = 0;
        }

        @Override
        protected String parseValue( ReadableBuffer value )
        {
            invocationCounter++;
            if ( invocationCounter == 1 )
            {
                testStore.state = newStoreState;
                throw new IllegalStateException( "Exception during state rotation." );
            }
            return testStore.readKey( value );
        }

        int getInvocationCounter()
        {
            return invocationCounter;
        }
    }

    @Rotation( Rotation.Strategy.INCREMENTING )
    class Store extends AbstractKeyValueStore<String>
    {
        private final HeaderField<?>[] headerFields;
        final IOFunction<Long,Long> rotation = new IOFunction<Long,Long>()
        {
            @Override
            public Long apply( Long version ) throws IOException
            {
                return prepareRotation( version ).rotate();
            }
        };

        private Store( HeaderField<?>... headerFields )
        {
            this( TimeUnit.MINUTES.toMillis( 10 ), headerFields );
        }

        private Store( long rotationTimeout, HeaderField<?>... headerFields )
        {
            super( resourceManager.fileSystem(), resourceManager.pageCache(), resourceManager.testPath(), null,
                    new RotationTimerFactory( Clock.SYSTEM_CLOCK, rotationTimeout ), 16, 16, headerFields );
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
        protected void updateHeaders( Headers.Builder headers, long version )
        {
            headers.put( TX_ID, version );
        }

        @Override
        protected long version( Headers headers )
        {
            Long transactionId = headers.get( TX_ID );
            return Math.max( TransactionIdStore.BASE_TX_ID,
                    transactionId != null ? transactionId.longValue() : TransactionIdStore.BASE_TX_ID );
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
