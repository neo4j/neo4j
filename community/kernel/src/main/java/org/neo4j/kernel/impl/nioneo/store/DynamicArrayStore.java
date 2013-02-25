/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import static java.lang.System.arraycopy;

import java.io.File;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.neo4j.helpers.Pair;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.windowpool.WindowPoolFactory;
import org.neo4j.kernel.impl.util.Bits;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * Dynamic store that stores strings.
 */
public class DynamicArrayStore extends AbstractDynamicStore
{
    static final int NUMBER_HEADER_SIZE = 3;
    static final int STRING_HEADER_SIZE = 5;
    
    // store version, each store ends with this string (byte encoded)
    public static final String TYPE_DESCRIPTOR = "ArrayPropertyStore";
    public static final String VERSION = buildTypeDescriptorAndVersion( TYPE_DESCRIPTOR );

    public DynamicArrayStore(File fileName, Config configuration, IdType idType,
                             IdGeneratorFactory idGeneratorFactory, WindowPoolFactory windowPoolFactory,
                             FileSystemAbstraction fileSystemAbstraction, StringLogger stringLogger)
    {
        super( fileName, configuration, idType, idGeneratorFactory, windowPoolFactory,
                fileSystemAbstraction, stringLogger);
    }
    
    @Override
    public void accept( RecordStore.Processor processor, DynamicRecord record )
    {
        processor.processArray( this, record );
    }

    @Override
    public String getTypeDescriptor()
    {
        return TYPE_DESCRIPTOR;
    }
    
    private Collection<DynamicRecord> allocateFromNumbers( Object array, Iterator<DynamicRecord> recordsToUseFirst )
    {
        Class<?> componentType = array.getClass().getComponentType();
        boolean isPrimitiveByteArray = componentType.equals( Byte.TYPE );
        boolean isByteArray = componentType.equals( Byte.class ) || isPrimitiveByteArray;
        byte[] bytes = null;
        ShortArray type = ShortArray.typeOf( array );
        if ( type == null ) throw new IllegalArgumentException( array + " not a valid array type." );
        
        int arrayLength = Array.getLength( array );
        int requiredBits = isByteArray ? Byte.SIZE : type.calculateRequiredBitsForArray( array, arrayLength);
        int totalBits = requiredBits*arrayLength;
        int numberOfBytes = (totalBits-1)/8+1;
        int bitsUsedInLastByte = totalBits%8;
        bitsUsedInLastByte = bitsUsedInLastByte == 0 ? 8 : bitsUsedInLastByte;
        numberOfBytes += NUMBER_HEADER_SIZE; // type + rest + requiredBits header. TODO no need to use full bytes
        int length = arrayLength;
        if ( isByteArray )
        {
            bytes = new byte[NUMBER_HEADER_SIZE+length];
            bytes[0] = (byte) type.intValue();
            bytes[1] = (byte) bitsUsedInLastByte;
            bytes[2] = (byte) requiredBits;
            if ( isPrimitiveByteArray ) arraycopy( array, 0, bytes, NUMBER_HEADER_SIZE, length );
            else
            {
                Byte[] source = (Byte[]) array;
                for ( int i = 0; i < source.length; i++ ) bytes[NUMBER_HEADER_SIZE+i] = source[i].byteValue();
            }
        }
        else
        {
            Bits bits = Bits.bits( numberOfBytes );
            bits.put( (byte)type.intValue() );
            bits.put( (byte)bitsUsedInLastByte );
            bits.put( (byte)requiredBits );
            type.writeAll(array,length,requiredBits,bits);
            bytes = bits.asBytes();
        }
        return allocateRecordsFromBytes( bytes, recordsToUseFirst );
    }

    private Collection<DynamicRecord> allocateFromString( String[] array, Iterator<DynamicRecord> recordsToUseFirst )
    {
        List<byte[]> stringsAsBytes = new ArrayList<byte[]>();
        int totalBytesRequired = STRING_HEADER_SIZE; // 1b type + 4b array length
        for ( String string : array )
        {
            byte[] bytes = PropertyStore.encodeString( string );
            stringsAsBytes.add( bytes );
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
        return allocateRecordsFromBytes( buf.array(), recordsToUseFirst );
    }

    public Collection<DynamicRecord> allocateRecords( Object array )
    {
        return allocateRecords( array, Collections.<DynamicRecord>emptyList().iterator() );
    }
    
    public Collection<DynamicRecord> allocateRecords( Object array, Iterator<DynamicRecord> recordsToUseFirst )
    {
        if ( !array.getClass().isArray() )
        {
            throw new IllegalArgumentException( array + " not an array" );
        }

        Class<?> type = array.getClass().getComponentType();
        if ( type.equals( String.class ) )
        {
            return allocateFromString( (String[]) array, recordsToUseFirst );
        }
        else
        {
            return allocateFromNumbers( array, recordsToUseFirst );
        }
    }

    public Object getRightArray( Pair<byte[],byte[]> data )
    {
        byte[] header = data.first();
        byte[] bArray = data.other();
        byte typeId = header[0];
        if ( typeId == PropertyType.STRING.intValue() )
        {
            ByteBuffer headerBuffer = ByteBuffer.wrap( header, 1/*skip the type*/, header.length-1 );
            int arrayLength = headerBuffer.getInt();
            String[] result = new String[arrayLength];
            
            ByteBuffer dataBuffer = ByteBuffer.wrap( bArray );
            for ( int i = 0; i < arrayLength; i++ )
            {
                int byteLength = dataBuffer.getInt();
                byte[] stringByteArray = new byte[byteLength];
                dataBuffer.get( stringByteArray );
                result[i] = (String) PropertyStore.decodeString( stringByteArray );
            }
            return result;
        }
        else
        {
            ShortArray type = ShortArray.typeOf( typeId );
            int bitsUsedInLastByte = header[1];
            int requiredBits = header[2];
            if ( requiredBits == 0 )
                return type.createEmptyArray();
            Object result = null;
            if ( type == ShortArray.BYTE && requiredBits == Byte.SIZE )
            {   // Optimization for byte arrays (probably large ones)
                result = bArray;
            }
            else
            {   // Fallback to the generic approach, which is a slower
                Bits bits = Bits.bitsFromBytes( bArray );
                int length = (bArray.length*8-(8-bitsUsedInLastByte))/requiredBits;
                result = type.createArray(length, bits, requiredBits);
            }
            return result;
        }
    }

    public Object getArrayFor( Iterable<DynamicRecord> records )
    {
        return getRightArray( readFullByteArray( records, PropertyType.ARRAY ) );
    }
}
