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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Pair;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.PageCacheRule;
import org.neo4j.test.ResourceRule;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.impl.store.kvstore.KeyValueStoreFileFormatTest.Data.data;
import static org.neo4j.kernel.impl.store.kvstore.KeyValueStoreFileFormatTest.DataEntry.entry;
import static org.neo4j.test.ResourceRule.testPath;

public class KeyValueStoreFileFormatTest
{
    public final @Rule EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    public final @Rule PageCacheRule pages = new PageCacheRule();
    public final @Rule ResourceRule<File> storeFile = testPath();

    @Before
    public void existingStoreDirectory()
    {
        fs.get().mkdirs( storeFile.get().getParentFile() );
    }

    @Test
    public void shouldCreateAndOpenEmptyStoreWithEmptyHeader() throws Exception
    {
        // given
        Format format = new Format();

        // when
        format.createEmpty( noHeaders() );

        // then
        try ( KeyValueStoreFile file = format.open() )
        {
            assertTrue( file.headers().fields().isEmpty() );
            assertEntries( 0, file );
        }
    }

    @Test
    public void shouldCreateAndOpenEmptyStoreWithHeader() throws Exception
    {
        // given
        Format format = new Format( "foo", "bar" );
        Map<String,byte[]> headers = new HashMap<>();
        headers.put( "foo", new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 'f', 'o', 'o'} );
        headers.put( "bar", new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 'b', 'a', 'r'} );

        // when
        format.createEmpty( headers );

        // then
        try ( KeyValueStoreFile file = format.open() )
        {
            assertDeepEquals( headers, file.headers() );
            assertEntries( 0, file );
        }
    }

    @Test
    public void shouldCreateAndOpenStoreWithNoDataAndEmptyHeader() throws Exception
    {
        // given
        Format format = new Format();

        // when
        try ( KeyValueStoreFile file = format.create( noHeaders(), noData() ) )
        // then
        {
            assertTrue( file.headers().fields().isEmpty() );
            assertEntries( 0, file );
        }
    }

    @Test
    public void shouldCreateAndOpenStoreWithNoDataWithHeader() throws Exception
    {
        // given
        Format format = new Format( "abc", "xyz" );
        Map<String,byte[]> headers = new HashMap<>();
        headers.put( "abc", new byte[]{'h', 'e', 'l', 'l', 'o', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,} );
        headers.put( "xyz", new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 'w', 'o', 'r', 'l', 'd',} );

        // when
        try ( KeyValueStoreFile file = format.create( headers, noData() ) )
        // then
        {
            assertDeepEquals( headers, file.headers() );
            assertEntries( 0, file );
        }
    }

    @Test
    public void shouldCreateAndOpenStoreWithDataAndEmptyHeader() throws Exception
    {
        // given
        Format format = new Format();
        Data data = data(
                entry( new byte[]{'o', 'n', 'e'}, new byte[]{'a', 'l', 'p', 'h', 'a'} ),
                entry( new byte[]{'t', 'w', 'o'}, new byte[]{'b', 'e', 't', 'a'} ),
                entry( new byte[]{'z', 'e', 'd'}, new byte[]{'o', 'm', 'e', 'g', 'a'} ) );

        // when
        try ( KeyValueStoreFile file = format.create( noHeaders(), data ) )
        // then
        {
            assertTrue( file.headers().fields().isEmpty() );
            file.scan( expectData( data ) );
            assertEquals( "number of entries", 3, data.index );
            assertEntries( 3, file );
        }
    }

    @Test
    public void shouldCreateAndOpenStoreWithDataAndHeader() throws Exception
    {
        // given
        Format format = new Format( "abc", "xyz" );
        Map<String,byte[]> headers = new HashMap<>();
        headers.put( "abc", new byte[]{'h', 'e', 'l', 'l', 'o', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,} );
        headers.put( "xyz", new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 'w', 'o', 'r', 'l', 'd',} );
        Data data = data(
                entry( new byte[]{'o', 'n', 'e'}, new byte[]{'a', 'l', 'p', 'h', 'a'} ),
                entry( new byte[]{'t', 'w', 'o'}, new byte[]{'b', 'e', 't', 'a'} ),
                entry( new byte[]{'z', 'e', 'd'}, new byte[]{'o', 'm', 'e', 'g', 'a'} ) );

        // when
        try ( KeyValueStoreFile file = format.create( headers, data ) )
        // then
        {
            assertDeepEquals( headers, file.headers() );
            file.scan( expectData( data ) );
            assertEquals( "number of entries", 3, data.index );
            assertEntries( 3, file );
        }
    }

    @Test
    public void shouldFindEntriesInFile() throws Exception
    {
        // given
        Format format = new Format( "one", "two" );
        Map<String,byte[]> headers = new HashMap<>();
        headers.put( "one", new byte[]{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,} );
        headers.put( "two", new byte[]{2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,} );
        Map<String,String> config = new HashMap<>();
        config.put( GraphDatabaseSettings.pagecache_memory.name(), "8M" );
        config.put( GraphDatabaseSettings.mapped_memory_page_size.name(), "256" );
        Data data = data(
                // page 0
                entry( bytes( 17 ), bytes( 'v', 'a', 'l', 1 ) ),
                entry( bytes( 22 ), bytes( 'v', 'a', 'l', 2 ) ),
                entry( bytes( 22 ), bytes( 'v', 'a', 'l', 3 ) ),
                entry( bytes( 25 ), bytes( 'v', 'a', 'l', 4 ) ),
                entry( bytes( 27 ), bytes( 'v', 'a', 'l', 5 ) ),
                // page 1
                entry( bytes( 27 ), bytes( 'v', 'a', 'l', 6 ) ),
                entry( bytes( 31 ), bytes( 'v', 'a', 'l', 7 ) ),
                entry( bytes( 63 ), bytes( 'v', 'a', 'l', 8 ) ),
                entry( bytes( 127 ), bytes( 'v', 'a', 'l', 9 ) ),
                entry( bytes( 255 ), bytes( 'v', 'a', 'l', 10 ) ),
                entry( bytes( 511 ), bytes( 'v', 'a', 'l', 11 ) ),
                entry( bytes( 1023 ), bytes( 'v', 'a', 'l', 12 ) ),
                entry( bytes( 1050 ), bytes( 'v', 'a', 'l', 13 ) ),
                // page 2
                entry( bytes( 2000 ), bytes( 'v', 'a', 'l', 14 ) ) );

        // when
        try ( KeyValueStoreFile file = format.create( config, headers, data ) )
        // then
        {
            assertFind( file, 17, 17, true, new Bytes( 'v', 'a', 'l', 1 ) );
            assertFind( file, 22, 22, true, new Bytes( 'v', 'a', 'l', 2 ), new Bytes( 'v', 'a', 'l', 3 ) );
            assertFind( file, 25, 25, true, new Bytes( 'v', 'a', 'l', 4 ) );
            assertFind( file, 27, 27, true, new Bytes( 'v', 'a', 'l', 5 ), new Bytes( 'v', 'a', 'l', 6 ) );
            assertFind( file, 26, 30, false, new Bytes( 'v', 'a', 'l', 5 ), new Bytes( 'v', 'a', 'l', 6 ) );
            assertFind( file, 31, 31, true, new Bytes( 'v', 'a', 'l', 7 ) );
            assertFind( file, 32, 1024, false,
                    new Bytes( 'v', 'a', 'l', 8 ),
                    new Bytes( 'v', 'a', 'l', 9 ),
                    new Bytes( 'v', 'a', 'l', 10 ),
                    new Bytes( 'v', 'a', 'l', 11 ),
                    new Bytes( 'v', 'a', 'l', 12 ) );
            assertFind( file, 1050, 1050, true, new Bytes( 'v', 'a', 'l', 13 ) );
            assertFind( file, 2000, 2000, true, new Bytes( 'v', 'a', 'l', 14 ) );
            assertFind( file, 1500, 8000, false, new Bytes( 'v', 'a', 'l', 14 ) );
            assertFind( file, 1050, 8000, true, new Bytes( 'v', 'a', 'l', 13 ), new Bytes( 'v', 'a', 'l', 14 ) );
            assertFind( file, 2001, Integer.MAX_VALUE, false );
        }
    }

    @Test
    public void shouldNotFindAnythingWhenSearchKeyIsAfterTheLastKey() throws Exception
    {
        // given
        Format format = new Format();
        Map<String,byte[]> metadata = new HashMap<>();
        Map<String,String> config = new HashMap<>();
        config.put( GraphDatabaseSettings.pagecache_memory.name(), "8M" );
        config.put( GraphDatabaseSettings.mapped_memory_page_size.name(), "128" );
        Data data = data( // two full pages (and nothing more)
                // page 0
                entry( bytes( 12 ), bytes( 'v', 'a', 'l', 1 ) ),
                entry( bytes( 13 ), bytes( 'v', 'a', 'l', 2 ) ),
                // page 1
                entry( bytes( 15 ), bytes( 'v', 'a', 'l', 3 ) ),
                entry( bytes( 16 ), bytes( 'v', 'a', 'l', 4 ) ),
                entry( bytes( 17 ), bytes( 'v', 'a', 'l', 5 ) ),
                entry( bytes( 18 ), bytes( 'v', 'a', 'l', 6 ) ) );

        // when
        try ( KeyValueStoreFile file = format.create( config, metadata, data ) )
        // then
        {
            assertFind( file, 14, 15, false, new Bytes( 'v', 'a', 'l', 3 ) ); // after the first page
            assertFind( file, 19, 25, false ); // after the second page
            assertFind( file, 18, 25, true, new Bytes( 'v', 'a', 'l', 6 ) ); // last entry of the last page
        }
    }

    @Test
    public void shouldTruncateTheFile() throws Exception
    {
        Map<String,String> config = new HashMap<>();
        config.put( GraphDatabaseSettings.pagecache_memory.name(), "8M" );
        config.put( GraphDatabaseSettings.mapped_memory_page_size.name(), "128" );

        // given a well written file
        {
            Format format = new Format( "one", "two" );
            Map<String,byte[]> headers = new HashMap<>();
            headers.put( "one", new byte[]{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,} );
            headers.put( "two", new byte[]{2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,} );


            Data data = data( // two full pages (and nothing more)
                    // page 0
                    entry( bytes( 12 ), bytes( 'v', 'a', 'l', 1 ) ),
                    entry( bytes( 13 ), bytes( 'v', 'a', 'l', 2 ) ),
                    // page 1
                    entry( bytes( 15 ), bytes( 'v', 'a', 'l', 3 ) ),
                    entry( bytes( 16 ), bytes( 'v', 'a', 'l', 4 ) ),
                    entry( bytes( 17 ), bytes( 'v', 'a', 'l', 5 ) ),
                    entry( bytes( 18 ), bytes( 'v', 'a', 'l', 6 ) ) );

            try ( KeyValueStoreFile ignored = format.create( config, headers, data ) )
            {
            }
        }


        {
            // when failing on creating the next version of that file
            Format format = new Format( "three", "four" );
            Map<String,byte[]> headers = new HashMap<>();
            headers.put( "three", new byte[]{3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,} );
            headers.put( "four", new byte[]{4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,} );

            DataProvider data = new DataProvider()
            {
                @Override
                public void close() throws IOException
                {
                }

                @Override
                public boolean visit( WritableBuffer key, WritableBuffer value ) throws IOException
                {
                    throw new IOException( "boom!" );
                }
            };

            try ( KeyValueStoreFile ignored = format.create( config, headers, data ) )
            {
            }
            catch ( IOException io )
            {
                // then only headers are present in the file and not the old content
                assertEquals( "boom!", io.getMessage() );
                assertFormatSpecifierAndHeadersOnly( headers, fs.get(), storeFile.get() );
            }
        }
    }

    private void assertFormatSpecifierAndHeadersOnly( Map<String,byte[]> headers, FileSystemAbstraction fs, File file )
            throws IOException
    {
        assertTrue( fs.fileExists( file ) );
        try ( InputStream stream = fs.openAsInputStream( file ) )
        {
            // format specifier
            int read;
            int size = 16;
            byte[] readEntry = new byte[size];
            byte[] allZeros = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

            read = stream.read( readEntry );
            assertEquals( size, read );
            assertArrayEquals( allZeros, readEntry );

            read = stream.read( readEntry );
            assertEquals( size, read );
            assertArrayEquals( new byte[]{-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1}, readEntry );


            for ( int i = 0; i < headers.size(); i++ )
            {
                read = stream.read( readEntry );
                assertEquals( size, read );
                assertArrayEquals( allZeros, readEntry );

                read = stream.read( readEntry );
                assertEquals( size, read );
                headers.containsValue( readEntry );
            }

            assertEquals( -1, stream.read() );
        }
    }

    private static void assertFind( KeyValueStoreFile file, int min, int max, boolean exact, Bytes... expected )
            throws IOException
    {
        Pair<Boolean,List<Bytes>> result = find( file, min, max );
        assertEquals( "exact match", exact, result.first() );
        assertEquals( String.format( "find(min=%d, max=%d)", min, max ), Arrays.asList( expected ), result.other() );
    }

    private static Pair<Boolean,List<Bytes>> find( KeyValueStoreFile file, final int min, final int max )
            throws IOException
    {
        final List<Bytes> values = new ArrayList<>();
        boolean result = file.scan( new SearchKey()
        {
            @Override
            public void searchKey( WritableBuffer key )
            {
                key.putInt( key.size() - 4, min );
            }
        }, new KeyValueVisitor()
        {
            @Override
            public boolean visit( ReadableBuffer key, ReadableBuffer value )
            {
                if ( key.getInt( key.size() - 4 ) <= max )
                {
                    values.add( new Bytes( value.get( 0, new byte[value.size()] ) ) );
                    return true;
                }
                return false;
            }
        } );
        return Pair.of( result, values );
    }

    static class Bytes
    {
        final byte[] bytes;

        Bytes( byte[] bytes )
        {
            this.bytes = bytes;
        }

        Bytes( int... data )
        {
            this.bytes = bytes( data );
        }

        @Override
        public String toString()
        {
            return Arrays.toString( bytes );
        }

        @Override
        public boolean equals( Object o )
        {
            return this == o || o instanceof Bytes && Arrays.equals( bytes, ((Bytes) o).bytes );
        }

        @Override
        public int hashCode()
        {
            return Arrays.hashCode( bytes );
        }
    }

    private static byte[] bytes( int... data )
    {
        if ( data.length > 4 )
        {
            throw new AssertionError( "Invalid usage; should have <= 4 data items, got: " + data.length );
        }
        byte[] result = new byte[16];
        for ( int d = data.length, r = result.length - 4; d-- > 0; r -= 4 )
        {
            int value = data[d];
            for ( int i = 4; i-- > 0; )
            {
                result[r + i] = (byte) (value & 0xFF);
                value >>>= 8;
            }
        }
        return result;
    }

    private void assertDeepEquals( Map<String,byte[]> expected, Headers actual )
    {
        try
        {
            int size = 0;
            for ( HeaderField<?> field : actual.fields() )
            {
                assertArrayEquals( field.toString(), expected.get( field.toString() ), (byte[]) actual.get( field ) );
                size++;
            }
            assertEquals( "number of headers", expected.size(), size );
        }
        catch ( AssertionError e )
        {
            System.out.println( actual );
            throw e;
        }
    }

    static void assertEntries( final int expected, KeyValueStoreFile file ) throws IOException
    {
        class Visitor implements KeyValueVisitor
        {
            int visited = 0;

            @Override
            public boolean visit( ReadableBuffer key, ReadableBuffer value )
            {
                if ( ++visited > expected )
                {
                    fail( "should not have more than " + expected + " data entries" );
                }
                return true;
            }

            void done()
            {
                assertEquals( "number of entries", expected, visited );
            }
        }
        Visitor visitor = new Visitor();
        file.scan( visitor );
        visitor.done();
    }

    static KeyValueVisitor expectData( final Data expected )
    {
        expected.index = 0; // reset the visitor
        return new KeyValueVisitor()
        {
            @Override
            public boolean visit( ReadableBuffer key, ReadableBuffer value )
            {
                byte[] expectedKey = new byte[key.size()];
                byte[] expectedValue = new byte[value.size()];
                if ( !expected.visit( new BigEndianByteArrayBuffer( expectedKey ),
                        new BigEndianByteArrayBuffer( expectedValue ) ) )
                {
                    return false;
                }
                assertEqualContent( expectedKey, key );
                return true;
            }
        };
    }

    static void assertEqualContent( byte[] expected, ReadableBuffer actual )
    {
        for ( int i = 0; i < expected.length; i++ )
        {
            if ( expected[i] != actual.getByte( i ) )
            {
                fail( "expected <" + Arrays.toString( expected ) + "> but was <" + actual + ">" );
            }
        }
    }

    class Format extends KeyValueStoreFileFormat
    {
        private final Map<String,HeaderField<byte[]>> headerFields = new HashMap<>();

        public Format( String... defaultHeaderFields )
        {
            this( StubCollector.headerFields( defaultHeaderFields ) );
        }

        private Format( HeaderField<byte[]>[] headerFields )
        {
            super( 32, headerFields );
            for ( HeaderField<byte[]> headerField : headerFields )
            {
                this.headerFields.put( headerField.toString(), headerField );
            }
        }

        void createEmpty( Map<String,byte[]> headers ) throws IOException
        {
            createEmptyStore( fs.get(), storeFile.get(), 16, 16, headers( headers ) );
        }

        KeyValueStoreFile create( Map<String,byte[]> headers, DataProvider data )
                throws IOException
        {
            return createStore( fs.get(), pages.getPageCache( fs.get() ), storeFile.get(), 16, 16, headers( headers ),
                    data );
        }

        KeyValueStoreFile create( Map<String,String> config, Map<String,byte[]> headers, DataProvider data )
                throws IOException
        {
            return createStore( fs.get(), pages.getPageCache( fs.get(), new Config( config ) ), storeFile.get(), 16, 16,
                    headers( headers ), data );
        }

        private Headers headers( Map<String,byte[]> headers )
        {
            Headers.Builder builder = Headers.headersBuilder();
            for ( Map.Entry<String,byte[]> entry : headers.entrySet() )
            {
                builder.put( headerFields.get( entry.getKey() ), entry.getValue() );
            }
            return builder.headers();
        }

        KeyValueStoreFile open() throws IOException
        {
            return openStore( fs.get(), pages.getPageCache( fs.get() ), storeFile.get() );
        }

        @Override
        protected void writeFormatSpecifier( WritableBuffer formatSpecifier )
        {
            for ( int i = 0; i < formatSpecifier.size(); i++ )
            {
                formatSpecifier.putByte( i, (byte) 0xFF );
            }
        }
    }

    static class Data implements DataProvider
    {
        static Data data( final DataEntry... data )
        {
            return new Data( data );
        }

        private final DataEntry[] data;
        private int index;

        private Data( DataEntry[] data )
        {
            this.data = data;
        }

        @Override
        public boolean visit( WritableBuffer key, WritableBuffer value )
        {
            if ( index < data.length )
            {
                DataEntry entry = data[index++];
                write( entry.key, key );
                write( entry.value, value );
                return true;
            }
            return false;
        }

        @Override
        public void close() throws IOException
        {
        }
    }

    static void write( byte[] source, WritableBuffer target )
    {
        for ( int i = 0; i < source.length; i++ )
        {
            target.putByte( i, source[i] );
        }
    }

    static DataProvider noData()
    {
        return new DataProvider()
        {
            @Override
            public boolean visit( WritableBuffer key, WritableBuffer value )
            {
                return false;
            }

            @Override
            public void close() throws IOException
            {
            }
        };
    }

    static class DataEntry
    {
        static DataEntry entry( byte[] key, byte[] value )
        {
            return new DataEntry( key, value );
        }

        final byte[] key, value;

        DataEntry( byte[] key, byte[] value )
        {
            this.key = key;
            this.value = value;
        }
    }

    static Map<String,byte[]> noHeaders()
    {
        return Collections.emptyMap();
    }
}
