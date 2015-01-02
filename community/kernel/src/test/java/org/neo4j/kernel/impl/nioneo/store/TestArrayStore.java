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
package org.neo4j.kernel.impl.nioneo.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.UTF8;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.DefaultTxHook;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.Bits;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.TargetDirectory;

public class TestArrayStore
{
    private File dir;
    private DynamicArrayStore arrayStore;
    
    @Before
    public void before() throws Exception
    {
        dir = TargetDirectory.forTest( getClass() ).cleanDirectory( "db" );
        Map<String, String> configParams = MapUtil.stringMap();
        Config config = new Config( configParams );
        DefaultIdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory();
        DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        StoreFactory factory = new StoreFactory( config,
                idGeneratorFactory, new DefaultWindowPoolFactory(), fs, StringLogger.DEV_NULL, new DefaultTxHook() );
        File fileName = new File( dir, "arraystore" );
        factory.createDynamicArrayStore( fileName, 120 );
        arrayStore = new DynamicArrayStore( fileName, config, IdType.ARRAY_BLOCK, idGeneratorFactory,
                new DefaultWindowPoolFactory(), fs, StringLogger.DEV_NULL );
    }

    @After
    public void after() throws Exception
    {
        if ( arrayStore != null )
            arrayStore.close();
    }
    
    @Test
    public void intArrayPropertiesShouldBeBitPacked() throws Exception
    {
        assertBitPackedArrayGetsCorrectlySerializedAndDeserialized( new int[] { 1, 2, 3, 4, 5, 6, 7 }, PropertyType.INT, 3, 1 );
        assertBitPackedArrayGetsCorrectlySerializedAndDeserialized( new int[] { 1, 2, 3, 4, 5, 6, 7, 8 }, PropertyType.INT, 4, 1 );
        assertBitPackedArrayGetsCorrectlySerializedAndDeserialized( new int[] { 1000, 10000, 13000 }, PropertyType.INT, 14, 2 );
    }
    
    @Test
    public void longArrayPropertiesShouldBeBitPacked() throws Exception
    {
        assertBitPackedArrayGetsCorrectlySerializedAndDeserialized( new long[] { 1, 2, 3, 4, 5, 6, 7 }, PropertyType.LONG, 3, 1 );
        assertBitPackedArrayGetsCorrectlySerializedAndDeserialized( new long[] { 1, 2, 3, 4, 5, 6, 7, 8 }, PropertyType.LONG, 4, 1 );
        assertBitPackedArrayGetsCorrectlySerializedAndDeserialized( new long[] { 1000, 10000, 13000, 15000000000L }, PropertyType.LONG, 34, 2 );
    }
    
    @Test
    public void byteArrayPropertiesShouldNotBeBitPacked() throws Exception
    {
        /* Byte arrays are always stored unpacked. For two reasons:
         * - They are very unlikely to gain anything from bit packing
         * - byte[] are often used for storing big arrays and the bigger the long
         *   any bit analysis would take. For both writing and reading */
        assertBitPackedArrayGetsCorrectlySerializedAndDeserialized( new byte[] { 1, 2, 3, 4, 5 }, PropertyType.BYTE, Byte.SIZE, 1 );
    }
    
    @Test
    public void stringArrayGetsStoredAsUtf8() throws Exception
    {
        String[] array = new String[] { "first", "second" };
        Collection<DynamicRecord> records = arrayStore.allocateRecords( array );
        Pair<byte[], byte[]> loaded = loadArray( records );
        assertStringHeader( loaded.first(), array.length );
        ByteBuffer buffer = ByteBuffer.wrap( loaded.other() );
        for ( String item : array )
        {
            byte[] expectedData = UTF8.encode( item );
            assertEquals( expectedData.length, buffer.getInt() );
            byte[] loadedItem = new byte[expectedData.length];
            buffer.get( loadedItem );
            assertTrue( Arrays.equals( expectedData, loadedItem ) );
        }
    }
    
    private void assertStringHeader( byte[] header, int itemCount )
    {
        assertEquals( PropertyType.STRING.byteValue(), header[0] );
        assertEquals( itemCount, ByteBuffer.wrap( header, 1, 4 ).getInt() );
    }

    private void assertBitPackedArrayGetsCorrectlySerializedAndDeserialized( Object array, PropertyType type,
            int expectedBitsUsedPerItem, int expectedRecordCount )
    {
        Collection<DynamicRecord> records = storeArray( array );
        Pair<byte[], byte[]> asBytes = loadArray( records );
        assertArrayHeader( asBytes.first(), type, expectedBitsUsedPerItem );
        Bits bits = Bits.bitsFromBytes( asBytes.other() );
        int length = Array.getLength( array );
        for ( int i = 0; i < length; i++ )
            assertEquals( ((Number)Array.get( array, i )).longValue(), bits.getLong( expectedBitsUsedPerItem ) );
    }
    
    private void assertArrayHeader( byte[] header, PropertyType type, int bitsPerItem )
    {
        assertEquals( type.byteValue(), header[0] );
        assertEquals( bitsPerItem, header[2] );
    }

    private Collection<DynamicRecord> storeArray( Object array )
    {
        Collection<DynamicRecord> records = arrayStore.allocateRecords( array );
        for ( DynamicRecord record : records )
            arrayStore.updateRecord( record );
        return records;
    }
    
    private Pair<byte[], byte[]> loadArray( Collection<DynamicRecord> records )
    {
        return arrayStore.readFullByteArray( records, PropertyType.ARRAY );
    }
}
