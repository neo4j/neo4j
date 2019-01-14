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

import java.io.File;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.Collection;

import org.neo4j.helpers.collection.Pair;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.format.Capability;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.UnsupportedFormatCapabilityException;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.util.Bits;
import org.neo4j.logging.LogProvider;
import org.neo4j.values.storable.CRSTable;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.lang.System.arraycopy;

/**
 * Dynamic store that stores arrays.
 *
 * Arrays are uniform collections of the same type. They can contain primitives, strings or Geometries.
 * <ul>
 *     <li>
 *         Primitive arrays are stored using a 3 byte header followed by a byte[] of bit-compacted data. The header defines the format of the byte[]:
 *         <ul>
 *             <li>Byte 0: The type of the primitive being stored. See {@link PropertyType}</li>
 *             <li>Byte 1: The number of bits used in the last byte</li>
 *             <li>Byte 2: The number of bits required for each element of the data array (after compaction)</li>
 *         </ul>
 *         The total number of elements can be calculated by combining the information about the individual element size
 *         (bits required - 3rd byte) with the length of the data specified in the DynamicRecordFormat.
 *     </li>
 *     <li>
 *         Arrays of strings are stored using a 5 byte header:
 *         <ul>
 *             <li>Byte 0: PropertyType.STRING</li>
 *             <li>Bytes 1 to 4: 32bit Int length of string array</li>
 *         </ul>
 *         This is followed by a byte[] composed of a 4 byte header containing the length of the byte[] representstion of the string, and then those bytes.
 *     </li>
 *     <li>
 *         Arrays of Geometries starting with a 6 byte header:
 *         <ul>
 *             <li>Byte 0: PropertyType.GEOMETRY</li>
 *             <li>Byte 1: GeometryType, currently only POINT is supported</li>
 *             <li>Byte 2: The dimension of the geometry (currently only 2 or 3 dimensions are supported)</li>
 *             <li>Byte 3: Coordinate Reference System Table id: {@link CRSTable}</li>
 *             <li>Bytes 4-5: 16bit short Coordinate Reference System code: {@link CoordinateReferenceSystem}</li>
 *         </ul>
 *         The format of the body is specific to the type of Geometry being stored:
 *         <ul>
 *             <li>Points: Stored as double[] using the same format as primitive arrays above, starting with the 3 byte header (see above)</li>
 *         </ul>
 *     </li>
 * </ul>
 */
public class DynamicArrayStore extends AbstractDynamicStore
{
    public static final int NUMBER_HEADER_SIZE = 3;
    public static final int STRING_HEADER_SIZE = 5;
    public static final int GEOMETRY_HEADER_SIZE = 6;   // This should match contents of GeometryType.GeometryHeader
    public static final int TEMPORAL_HEADER_SIZE = 2;

    // store version, each store ends with this string (byte encoded)
    public static final String TYPE_DESCRIPTOR = "ArrayPropertyStore";
    private final boolean allowStorePointsAndTemporal;

    public DynamicArrayStore(
            File fileName,
            Config configuration,
            IdType idType,
            IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache,
            LogProvider logProvider,
            int dataSizeFromConfiguration,
            RecordFormats recordFormats,
            OpenOption... openOptions )
    {
        super( fileName, configuration, idType, idGeneratorFactory, pageCache,
                logProvider, TYPE_DESCRIPTOR, dataSizeFromConfiguration, recordFormats.dynamic(), recordFormats.storeVersion(), openOptions );
        allowStorePointsAndTemporal = recordFormats.hasCapability( Capability.POINT_PROPERTIES )
                && recordFormats.hasCapability( Capability.TEMPORAL_PROPERTIES );
    }

    @Override
    public <FAILURE extends Exception> void accept( RecordStore.Processor<FAILURE> processor, DynamicRecord record )
            throws FAILURE
    {
        processor.processArray( this, record );
    }

    public static byte[] encodeFromNumbers( Object array, int offsetBytes )
    {
        ShortArray type = ShortArray.typeOf( array );
        if ( type == null )
        {
            throw new IllegalArgumentException( array + " not a valid array type." );
        }

        if ( type == ShortArray.DOUBLE || type == ShortArray.FLOAT )
        {
            // Skip array compaction for floating point numbers where compaction makes very little difference
            return createUncompactedArray( type, array, offsetBytes );
        }
        else
        {
            return createBitCompactedArray( type, array, offsetBytes );
        }
    }

    private static byte[] createBitCompactedArray( ShortArray type, Object array, int offsetBytes )
    {
        Class<?> componentType = array.getClass().getComponentType();
        boolean isPrimitiveByteArray = componentType.equals( Byte.TYPE );
        boolean isByteArray = componentType.equals( Byte.class ) || isPrimitiveByteArray;
        int arrayLength = Array.getLength( array );
        int requiredBits = isByteArray ? Byte.SIZE : type.calculateRequiredBitsForArray( array, arrayLength );
        int totalBits = requiredBits * arrayLength;
        int bitsUsedInLastByte = totalBits % 8;
        bitsUsedInLastByte = bitsUsedInLastByte == 0 ? 8 : bitsUsedInLastByte;
        if ( isByteArray )
        {
            return createBitCompactedByteArray( type, isPrimitiveByteArray, array, bitsUsedInLastByte, requiredBits, offsetBytes );
        }
        else
        {
            int numberOfBytes = (totalBits - 1) / 8 + 1;
            numberOfBytes += NUMBER_HEADER_SIZE; // type + rest + requiredBits header. TODO no need to use full bytes
            Bits bits = Bits.bits( numberOfBytes );
            bits.put( (byte) type.intValue() );
            bits.put( (byte) bitsUsedInLastByte );
            bits.put( (byte) requiredBits );
            type.writeAll( array, arrayLength, requiredBits, bits );
            return bits.asBytes( offsetBytes );
        }
    }

    private static byte[] createBitCompactedByteArray( ShortArray type, boolean isPrimitiveByteArray, Object array,
            int bitsUsedInLastByte, int requiredBits, int offsetBytes )
    {
        int arrayLength = Array.getLength( array );
        byte[] bytes = new byte[NUMBER_HEADER_SIZE + arrayLength + offsetBytes];
        bytes[offsetBytes + 0] = (byte) type.intValue();
        bytes[offsetBytes + 1] = (byte) bitsUsedInLastByte;
        bytes[offsetBytes + 2] = (byte) requiredBits;
        if ( isPrimitiveByteArray )
        {
            arraycopy( array, 0, bytes, NUMBER_HEADER_SIZE + offsetBytes, arrayLength );
        }
        else
        {
            Byte[] source = (Byte[]) array;
            for ( int i = 0; i < source.length; i++ )
            {
                bytes[NUMBER_HEADER_SIZE + offsetBytes + i] = source[i];
            }
        }
        return bytes;
    }

    private static byte[] createUncompactedArray( ShortArray type, Object array, int offsetBytes )
    {
        int arrayLength = Array.getLength( array );
        int bytesPerElement = type.maxBits / 8;
        byte[] bytes = new byte[NUMBER_HEADER_SIZE + bytesPerElement * arrayLength + offsetBytes];
        bytes[offsetBytes + 0] = (byte) type.intValue();
        bytes[offsetBytes + 1] = (byte) 8;
        bytes[offsetBytes + 2] = (byte) type.maxBits;
        type.writeAll( array, bytes, NUMBER_HEADER_SIZE + offsetBytes );
        return bytes;
    }

    public static void allocateFromNumbers( Collection<DynamicRecord> target, Object array,
            DynamicRecordAllocator recordAllocator )
    {
        byte[] bytes = encodeFromNumbers( array, 0 );
        allocateRecordsFromBytes( target, bytes, recordAllocator );
    }

    private static void allocateFromCompositeType(
            Collection<DynamicRecord> target,
            byte[] bytes,
            DynamicRecordAllocator recordAllocator,
            boolean allowsStorage,
            Capability storageCapability )
    {
        if ( allowsStorage )
        {
            allocateRecordsFromBytes( target, bytes, recordAllocator );
        }
        else
        {
            throw new UnsupportedFormatCapabilityException( storageCapability );
        }
    }

    private static void allocateFromString( Collection<DynamicRecord> target, String[] array,
            DynamicRecordAllocator recordAllocator )
    {
        byte[][] stringsAsBytes = new byte[array.length][];
        int totalBytesRequired = STRING_HEADER_SIZE; // 1b type + 4b array length
        for ( int i = 0; i < array.length; i++ )
        {
            String string = array[i];
            byte[] bytes = PropertyStore.encodeString( string );
            stringsAsBytes[i] = bytes;
            totalBytesRequired += 4/*byte[].length*/ + bytes.length;
        }

        ByteBuffer buf = ByteBuffer.allocate( totalBytesRequired );
        buf.put( PropertyType.STRING.byteValue() );
        buf.putInt( array.length );
        for ( byte[] stringAsBytes : stringsAsBytes )
        {
            buf.putInt( stringAsBytes.length );
            buf.put( stringAsBytes );
        }
        allocateRecordsFromBytes( target, buf.array(), recordAllocator );
    }

    public void allocateRecords( Collection<DynamicRecord> target, Object array )
    {
        allocateRecords( target, array, this, allowStorePointsAndTemporal );
    }

    public static void allocateRecords( Collection<DynamicRecord> target, Object array,
            DynamicRecordAllocator recordAllocator, boolean allowStorePointsAndTemporal )
    {
        if ( !array.getClass().isArray() )
        {
            throw new IllegalArgumentException( array + " not an array" );
        }

        Class<?> type = array.getClass().getComponentType();
        if ( type.equals( String.class ) )
        {
            allocateFromString( target, (String[]) array, recordAllocator );
        }
        else if ( type.equals( PointValue.class ) )
        {
            allocateFromCompositeType( target,GeometryType.encodePointArray( (PointValue[]) array ),
                    recordAllocator, allowStorePointsAndTemporal, Capability.POINT_PROPERTIES );
        }
        else if ( type.equals( LocalDate.class ) )
        {
            allocateFromCompositeType( target, TemporalType.encodeDateArray( (LocalDate[]) array ),
                    recordAllocator, allowStorePointsAndTemporal, Capability.TEMPORAL_PROPERTIES );
        }
        else if ( type.equals( LocalTime.class ) )
        {
            allocateFromCompositeType( target, TemporalType.encodeLocalTimeArray( (LocalTime[]) array ),
                    recordAllocator, allowStorePointsAndTemporal, Capability.TEMPORAL_PROPERTIES );
        }
        else if ( type.equals( LocalDateTime.class ) )
        {
            allocateFromCompositeType( target, TemporalType.encodeLocalDateTimeArray( (LocalDateTime[]) array ),
                    recordAllocator, allowStorePointsAndTemporal, Capability.TEMPORAL_PROPERTIES );
        }
        else if ( type.equals( OffsetTime.class ) )
        {
            allocateFromCompositeType( target, TemporalType.encodeTimeArray( (OffsetTime[]) array ),
                    recordAllocator, allowStorePointsAndTemporal, Capability.TEMPORAL_PROPERTIES );
        }
        else if ( type.equals( ZonedDateTime.class ) )
        {
            allocateFromCompositeType( target, TemporalType.encodeDateTimeArray( (ZonedDateTime[]) array ),
                    recordAllocator, allowStorePointsAndTemporal, Capability.TEMPORAL_PROPERTIES );
        }
        else if ( type.equals( DurationValue.class ) )
        {
            allocateFromCompositeType( target, TemporalType.encodeDurationArray( (DurationValue[]) array ),
                    recordAllocator, allowStorePointsAndTemporal, Capability.TEMPORAL_PROPERTIES );
        }
        else
        {
            allocateFromNumbers( target, array, recordAllocator );
        }
    }

    public static Value getRightArray( Pair<byte[],byte[]> data )
    {
        byte[] header = data.first();
        byte[] bArray = data.other();
        byte typeId = header[0];
        if ( typeId == PropertyType.STRING.intValue() )
        {
            ByteBuffer headerBuffer = ByteBuffer.wrap( header, 1/*skip the type*/, header.length - 1 );
            int arrayLength = headerBuffer.getInt();
            String[] result = new String[arrayLength];

            ByteBuffer dataBuffer = ByteBuffer.wrap( bArray );
            for ( int i = 0; i < arrayLength; i++ )
            {
                int byteLength = dataBuffer.getInt();
                byte[] stringByteArray = new byte[byteLength];
                dataBuffer.get( stringByteArray );
                result[i] = PropertyStore.decodeString( stringByteArray );
            }
            return Values.stringArray( result );
        }
        else if ( typeId == PropertyType.GEOMETRY.intValue() )
        {
            GeometryType.GeometryHeader geometryHeader = GeometryType.GeometryHeader.fromArrayHeaderBytes(header);
            return GeometryType.decodeGeometryArray( geometryHeader, bArray );
        }
        else if ( typeId == PropertyType.TEMPORAL.intValue() )
        {
            TemporalType.TemporalHeader temporalHeader = TemporalType.TemporalHeader.fromArrayHeaderBytes(header);
            return TemporalType.decodeTemporalArray( temporalHeader, bArray );
        }
        else
        {
            ShortArray type = ShortArray.typeOf( typeId );
            int bitsUsedInLastByte = header[1];
            int requiredBits = header[2];
            if ( requiredBits == 0 )
            {
                return type.createEmptyArray();
            }
            if ( type == ShortArray.BYTE && requiredBits == Byte.SIZE )
            {   // Optimization for byte arrays (probably large ones)
                return Values.byteArray( bArray );
            }
            else
            {   // Fallback to the generic approach, which is a slower
                Bits bits = Bits.bitsFromBytes( bArray );
                int length = (bArray.length * 8 - (8 - bitsUsedInLastByte)) / requiredBits;
                return type.createArray( length, bits, requiredBits );
            }
        }
    }

    public Object getArrayFor( Iterable<DynamicRecord> records )
    {
        return getRightArray( readFullByteArray( records, PropertyType.ARRAY ) ).asObject();
    }
}
