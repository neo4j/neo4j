/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.util.Bits;

/**
 * Dynamic store that stores strings.
 */
public class DynamicArrayStore extends AbstractDynamicStore
{
    // store version, each store ends with this string (byte encoded)
    static final String VERSION = "ArrayPropertyStore v0.A.0";
    public static final String TYPE_DESCRIPTOR = "ArrayPropertyStore";

    public DynamicArrayStore( String fileName, Map<?,?> config, IdType idType )
    {
        super( fileName, config, idType );
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

    public static void createStore( String fileName, int blockSize,
            IdGeneratorFactory idGeneratorFactory )
    {
        createEmptyStore( fileName, blockSize, VERSION, idGeneratorFactory, IdType.ARRAY_BLOCK );
    }

    private Collection<DynamicRecord> allocateFromNumbers( long startBlock, Object array )
    {
        ShortArray type = ShortArray.typeOf( array );
        if (type == null)
        {
            throw new IllegalArgumentException( array
                                                + " not a valid array type." );
        }
        int arrayLength = Array.getLength( array );
        int requiredBits = type.calculateRequiredBitsForArray( array );
        int totalBits = requiredBits*arrayLength;
        int bytes = (totalBits-1)/8+1;
        int bitsUsedInLastByte = totalBits%8;
        bitsUsedInLastByte = bitsUsedInLastByte == 0 ? 8 : bitsUsedInLastByte;
        bytes += 3; // type + rest + requiredBits header. TODO no need to use full bytes
        Bits bits = Bits.bits( bytes );
        bits.put( (byte)type.intValue() );
        bits.put( (byte)bitsUsedInLastByte );
        bits.put( (byte)requiredBits );
        int length = arrayLength;
        for ( int i = 0; i < length; i++ )
        {
            type.put( Array.get( array, i ), bits, requiredBits );
        }
        return allocateRecords( startBlock, bits.asBytes() );
    }

    private Collection<DynamicRecord> allocateFromString( long startBlock,
        String[] array )
    {
        List<byte[]> stringsAsBytes = new ArrayList<byte[]>();
        int totalBytesRequired = 1+4; // 1b type + 3b array length
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
        return allocateRecords( startBlock, buf.array() );
    }

    public Collection<DynamicRecord> allocateRecords( long startBlock, Object array )
    {
        if ( !array.getClass().isArray() )
        {
            throw new IllegalArgumentException( array + " not an array" );
        }

        Class<?> type = array.getClass().getComponentType();
        if ( type.equals( String.class ) )
        {
            return allocateFromString( startBlock, (String[]) array );
        }
        else
        {
            return allocateFromNumbers( startBlock, array );
        }
    }

    public Object getRightArray( byte[] bArray )
    {
        byte typeId = bArray[0];
        if ( typeId == PropertyType.STRING.intValue() )
        {
            ByteBuffer buf = ByteBuffer.wrap( bArray );
            buf.get(); // Get rid of the type byte that we've already read
            int arrayLength = buf.getInt();
            String[] result = new String[arrayLength];
            for ( int i = 0; i < arrayLength; i++ )
            {
                int byteLength = buf.getInt();
                byte[] stringByteArray = new byte[byteLength];
                buf.get( stringByteArray );
                result[i] = (String) PropertyStore.getStringFor( stringByteArray );
            }
            return result;
        }
        else
        {
            ShortArray type = ShortArray.typeOf( typeId );
            Bits bits = Bits.bitsFromBytes( bArray );
            bits.getByte(); // type, we already got it
            int bitsUsedInLastByte = bits.getByte();
            int requiredBits = bits.getByte();
            if ( requiredBits == 0 ) return type.createArray( 0 );
            int length = ((bArray.length-3)*8-(8-bitsUsedInLastByte))/requiredBits;
            Object result = type.createArray( length );
            for ( int i = 0; i < length; i++ )
            {
                type.get( result, i, bits, requiredBits );
            }
            return result;
        }
    }

}