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
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.helpers.Pair;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.lifecycle.Lifespan;

import static org.junit.Assert.assertEquals;

import static org.neo4j.kernel.impl.store.kvstore.Resources.InitialLifecycle.STARTED;
import static org.neo4j.kernel.impl.store.kvstore.Resources.TestPath.FILE_IN_EXISTING_DIRECTORY;

public class AbstractKeyValueStoreTest
{
    public final @Rule Resources the = new Resources( FILE_IN_EXISTING_DIRECTORY );

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
        store.rotate( Collections.<String, byte[]>emptyMap() );
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
        store.rotate( Collections.<String, byte[]>emptyMap() );

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
                super( "txId" );
            }

            @Override
            void initialMetadata( String key, BigEndianByteArrayBuffer buffer )
            {
                if ( "txId".equals( key ) )
                {
                    buffer.putLong( buffer.size() - 8, 1 );
                }
                else
                {
                    super.initialMetadata( key, buffer );
                }
            }

            @Override
            protected int compareMetadata( Map<String, byte[]> lhs, Map<String, byte[]> rhs )
            {
                return Long.compare( lastLong( lhs.get( "txId" ) ), lastLong( rhs.get( "txId" ) ) );
            }
        }
        try ( Lifespan life = new Lifespan() )
        {
            Store store = life.add( new Impl() );

            // when
            for ( int txId = 2; txId <= 10; txId++ )
            {
                store.rotate( Collections.singletonMap( "txId", txId( txId ) ) );
            }
        }

        // then
        try ( Lifespan life = new Lifespan() )
        {
            Store store = life.add( new Impl() );
            assertEquals( 10, lastLong( store.metadata().get( "txId" ) ) );
        }
    }

    @Test
    public void shouldNotPickCorruptStoreFile() throws Exception
    {
        // given
        Store store = new Store( "txId" )
        {
            @Override
            void initialMetadata( String key, BigEndianByteArrayBuffer buffer )
            {
                switch ( key )
                {
                case "txId":
                    buffer.putLong( buffer.size() - 8, 1 );
                    break;
                default:
                    super.initialMetadata( key, buffer );
                }
            }

            @Override
            protected int compareMetadata( Map<String, byte[]> lhs, Map<String, byte[]> rhs )
            {
                return Long.compare( lastLong( lhs.get( "txId" ) ), lastLong( rhs.get( "txId" ) ) );
            }
        };
        Field state = AbstractKeyValueStore.class.getDeclaredField( "state" );
        state.setAccessible( true );
        @SuppressWarnings("unchecked")
        RotationStrategy<Map<String, byte[]>> rotation = ((KeyValueStoreState.Stopped) state.get( store )).rotation;

        // when
        File[] files = new File[10];
        {
            Pair<File, KeyValueStoreFile<Map<String, byte[]>>> file = rotation.create();
            files[0] = file.first();
            for ( int txId = 2, i = 1; i < files.length; txId <<= 1, i++ )
            {
                KeyValueStoreFile<Map<String, byte[]>> old = file.other();
                final int data = txId;
                file = rotation.next( file.first(), Collections.singletonMap( "txId", txId( txId ) ), data(
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

            assertEquals( 64, lastLong( store.metadata().get( "txId" ) ) );
        }
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

    static byte[] txId( long txId )
    {
        return new BigEndianByteArrayBuffer( 16 ).putLong( 8, txId ).buffer;
    }

    static long lastLong( byte[] txIds )
    {
        BigEndianByteArrayBuffer lhsTxId = new BigEndianByteArrayBuffer( txIds );
        return lhsTxId.getLong( lhsTxId.size() - 8 );
    }

    @Rotation(Rotation.Strategy.INCREMENTING)
    class Store extends AbstractKeyValueStore<String, Map<String, byte[]>, Map<String, byte[]>>
    {
        private final HeaderField<Map<String, byte[]>, byte[]>[] headerFields;

        Store( String... metadataKeys )
        {
            this( StubCollector.headerFields( metadataKeys ) );
        }

        private Store( HeaderField<Map<String, byte[]>, byte[]>[] headerFields )
        {
            super( the.fileSystem(), the.pageCache(), the.testPath(), 16, 16, headerFields );
            this.headerFields = headerFields;
        }

        @Override
        protected Map<String, byte[]> initialMetadata()
        {
            Map<String, byte[]> metadata = new HashMap<>();
            for ( HeaderField<Map<String, byte[]>, byte[]> field : headerFields )
            {
                String key = field.toString();
                byte[] value = new byte[16];
                metadata.put( key, value );
                initialMetadata( key, new BigEndianByteArrayBuffer( value ) );
            }
            return metadata;
        }

        void initialMetadata( String key, BigEndianByteArrayBuffer buffer )
        {
            buffer.fill( (byte) 0xFF );
        }

        @Override
        protected int compareMetadata( Map<String, byte[]> lhs, Map<String, byte[]> rhs )
        {
            return 0;
        }

        @Override
        protected Map<String, byte[]> updateMetadata( Map<String, byte[]> metadata, Map<String, byte[]> changes )
        {
            Map<String, byte[]> result = new HashMap<>( metadata );
            result.putAll( changes );
            return result;
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
        protected Map<String, byte[]> buildMetadata( ReadableBuffer formatSpecifier, CollectedMetadata metadata )
        {
            return StubCollector.metadata( headerFields, metadata );
        }

        @Override
        protected String extractFileTrailer( Map<String, byte[]> metadata )
        {
            return "And that's all folks.";
        }

        @Override
        protected void writeFormatSpecifier( WritableBuffer formatSpecifier )
        {
            formatSpecifier.putByte( 0, (byte) 0xFF );
            formatSpecifier.putByte( formatSpecifier.size() - 1, (byte) 0xFF );
        }

        public void put( String key, final String value ) throws IOException
        {
            apply( new Update<String>( key )
            {
                @Override
                protected void update( WritableBuffer buffer )
                {
                    writeKey( value, buffer );
                }
            } );
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