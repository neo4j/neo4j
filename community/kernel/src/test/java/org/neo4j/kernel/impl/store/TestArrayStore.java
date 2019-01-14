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
package org.neo4j.kernel.impl.store;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.neo4j.helpers.collection.Pair;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.util.Bits;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.string.UTF8;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Values;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestArrayStore
{
    @ClassRule
    public static final PageCacheRule pageCacheRule = new PageCacheRule();
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();
    @Rule
    public final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    private DynamicArrayStore arrayStore;
    private NeoStores neoStores;

    @Before
    public void before()
    {
        File dir = testDirectory.graphDbDir();
        FileSystemAbstraction fs = fileSystemRule.get();
        DefaultIdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory( fs );
        PageCache pageCache = pageCacheRule.getPageCache( fs );
        StoreFactory factory = new StoreFactory( dir, Config.defaults(), idGeneratorFactory, pageCache, fs,
                NullLogProvider.getInstance(), EmptyVersionContextSupplier.EMPTY );
        neoStores = factory.openAllNeoStores( true );
        arrayStore = neoStores.getPropertyStore().getArrayStore();
    }

    @After
    public void after()
    {
        if ( neoStores != null )
        {
            neoStores.close();
        }
    }

    @Test
    public void intArrayPropertiesShouldBeBitPacked()
    {
        assertBitPackedArrayGetsCorrectlySerializedAndDeserialized( new int[] { 1, 2, 3, 4, 5, 6, 7 }, PropertyType.INT, 3 );
        assertBitPackedArrayGetsCorrectlySerializedAndDeserialized( new int[] { 1, 2, 3, 4, 5, 6, 7, 8 }, PropertyType.INT, 4 );
        assertBitPackedArrayGetsCorrectlySerializedAndDeserialized( new int[] { 1000, 10000, 13000 }, PropertyType.INT, 14 );
    }

    @Test
    public void longArrayPropertiesShouldBeBitPacked()
    {
        assertBitPackedArrayGetsCorrectlySerializedAndDeserialized( new long[] { 1, 2, 3, 4, 5, 6, 7 }, PropertyType.LONG, 3 );
        assertBitPackedArrayGetsCorrectlySerializedAndDeserialized( new long[] { 1, 2, 3, 4, 5, 6, 7, 8 }, PropertyType.LONG, 4 );
        assertBitPackedArrayGetsCorrectlySerializedAndDeserialized( new long[]{1000, 10000, 13000, 15000000000L},
                PropertyType.LONG, 34 );
    }

    @Test
    public void doubleArrayPropertiesShouldNotBeBitPacked()
    {
        //TODO Enabling right-trim would allow doubles that are integers, like 42.0, to pack well
        //While enabling the default left-trim would only allow some extreme doubles to pack, like Double.longBitsToDouble( 0x1L )

        // Test doubles that pack well with right-trim
        assertBitPackedArrayGetsCorrectlySerializedAndDeserialized(
                new double[]{0.0, -100.0, 100.0, 0.5},
                PropertyType.DOUBLE, 64 );
        // Test doubles that pack well with left-trim
        assertBitPackedArrayGetsCorrectlySerializedAndDeserialized(
                new double[]{Double.longBitsToDouble( 0x1L ), Double.longBitsToDouble( 0x8L )},
                PropertyType.DOUBLE, 64 );
    }

    @Test
    public void byteArrayPropertiesShouldNotBeBitPacked()
    {
        /* Byte arrays are always stored unpacked. For two reasons:
         * - They are very unlikely to gain anything from bit packing
         * - byte[] are often used for storing big arrays and the bigger the long
         *   any bit analysis would take. For both writing and reading */
        assertBitPackedArrayGetsCorrectlySerializedAndDeserialized( new byte[] { 1, 2, 3, 4, 5 }, PropertyType.BYTE, Byte.SIZE );
    }

    @Test
    public void stringArrayGetsStoredAsUtf8()
    {
        String[] array = new String[] { "first", "second" };
        Collection<DynamicRecord> records = new ArrayList<>();
        arrayStore.allocateRecords( records, array );
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

    @Test
    public void pointArraysOfWgs84()
    {
        PointValue[] array = new PointValue[]{
                Values.pointValue( CoordinateReferenceSystem.WGS84, -45.0, -45.0 ),
                Values.pointValue( CoordinateReferenceSystem.WGS84, 12.8, 56.3 )};
        int numberOfBitsUsedForDoubles = 64;

        assertPointArrayHasCorrectFormat( array, numberOfBitsUsedForDoubles );
    }

    @Test
    public void pointArraysOfCartesian()
    {
        PointValue[] array = new PointValue[]{
                Values.pointValue( CoordinateReferenceSystem.Cartesian, -100.0, -100.0 ),
                Values.pointValue( CoordinateReferenceSystem.Cartesian, 25.0, 50.5 )};
        int numberOfBitsUsedForDoubles = 64;

        assertPointArrayHasCorrectFormat( array, numberOfBitsUsedForDoubles );
    }

    @Test( expected = IllegalArgumentException.class )
    public void pointArraysOfMixedCRS()
    {
        PointValue[] array =
                new PointValue[]{
                Values.pointValue( CoordinateReferenceSystem.Cartesian,
                        Double.longBitsToDouble( 0x1L ), Double.longBitsToDouble( 0x7L ) ),
                        Values.pointValue( CoordinateReferenceSystem.WGS84,
                                Double.longBitsToDouble( 0x1L ),
                                Double.longBitsToDouble( 0x1L ) )};

        Collection<DynamicRecord> records = new ArrayList<>();
        arrayStore.allocateRecords( records, array );
    }

    @Test( expected = IllegalArgumentException.class )
    public void pointArraysOfMixedDimension()
    {
        PointValue[] array =
                new PointValue[]{
                Values.pointValue( CoordinateReferenceSystem.Cartesian,
                        Double.longBitsToDouble( 0x1L ),
                        Double.longBitsToDouble( 0x7L ) ),
                        Values.pointValue( CoordinateReferenceSystem.Cartesian,
                                Double.longBitsToDouble( 0x1L ),
                                Double.longBitsToDouble( 0x1L ),
                                Double.longBitsToDouble( 0x4L ) )};

        Collection<DynamicRecord> records = new ArrayList<>();
        arrayStore.allocateRecords( records, array );
    }

    private void assertPointArrayHasCorrectFormat( PointValue[] array, int numberOfBitsUsedForDoubles )
    {
        Collection<DynamicRecord> records = new ArrayList<>();
        arrayStore.allocateRecords( records, array );
        Pair<byte[],byte[]> loaded = loadArray( records );
        assertGeometryHeader( loaded.first(),
                GeometryType.GEOMETRY_POINT.getGtype(),
                2,
                array[0].getCoordinateReferenceSystem().getTable().getTableId(),
                array[0].getCoordinateReferenceSystem().getCode() );

        final int dimension = array[0].coordinate().length;
        double[] pointDoubles = new double[array.length * dimension];
        for ( int i = 0; i < pointDoubles.length; i++ )
        {
            pointDoubles[i] = array[i / dimension].coordinate()[i % dimension];
        }

        byte[] doubleHeader = Arrays.copyOf( loaded.other(), DynamicArrayStore.NUMBER_HEADER_SIZE );
        byte[] doubleBody = Arrays.copyOfRange( loaded.other(), DynamicArrayStore.NUMBER_HEADER_SIZE, loaded.other().length );
        assertNumericArrayHeaderAndContent( pointDoubles, PropertyType.DOUBLE, numberOfBitsUsedForDoubles, Pair.of( doubleHeader, doubleBody ) );
    }

    private void assertStringHeader( byte[] header, int itemCount )
    {
        assertEquals( PropertyType.STRING.byteValue(), header[0] );
        assertEquals( itemCount, ByteBuffer.wrap( header, 1, 4 ).getInt() );
    }

    private void assertGeometryHeader( byte[] header, int geometryTpe, int dimension, int crsTableId, int crsCode )
    {
        assertEquals( PropertyType.GEOMETRY.byteValue(), header[0] );
        assertEquals( geometryTpe, header[1] );
        assertEquals( dimension, header[2] );
        assertEquals( crsTableId, header[3] );
        assertEquals( crsCode, ByteBuffer.wrap( header, 4, 2 ).getShort() );
    }

    private void assertBitPackedArrayGetsCorrectlySerializedAndDeserialized( Object array, PropertyType type,
            int expectedBitsUsedPerItem )
    {
        Collection<DynamicRecord> records = storeArray( array );
        Pair<byte[], byte[]> asBytes = loadArray( records );
        assertNumericArrayHeaderAndContent( array, type, expectedBitsUsedPerItem, asBytes );
    }

    private void assertNumericArrayHeaderAndContent( Object array, PropertyType type, int expectedBitsUsedPerItem, Pair<byte[],byte[]> loadedBytesFromStore )
    {
        assertArrayHeader( loadedBytesFromStore.first(), type, expectedBitsUsedPerItem );
        Bits bits = Bits.bitsFromBytes( loadedBytesFromStore.other() );
        int length = Array.getLength( array );
        for ( int i = 0; i < length; i++ )
        {
            if ( array instanceof double[] )
            {
                assertEquals( Double.doubleToLongBits( Array.getDouble( array, i ) ), bits.getLong( expectedBitsUsedPerItem ) );
            }
            else
            {
                assertEquals( Array.getLong( array, i ), bits.getLong( expectedBitsUsedPerItem ) );
            }
        }
    }

    private void assertArrayHeader( byte[] header, PropertyType type, int bitsPerItem )
    {
        assertEquals( type.byteValue(), header[0] );
        assertEquals( bitsPerItem, header[2] );
    }

    private Collection<DynamicRecord> storeArray( Object array )
    {
        Collection<DynamicRecord> records = new ArrayList<>();
        arrayStore.allocateRecords( records, array );
        for ( DynamicRecord record : records )
        {
            arrayStore.updateRecord( record );
        }
        return records;
    }

    private Pair<byte[], byte[]> loadArray( Collection<DynamicRecord> records )
    {
        return arrayStore.readFullByteArray( records, PropertyType.ARRAY );
    }
}
