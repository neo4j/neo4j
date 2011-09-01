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
import java.util.Collection;
import java.util.Map;

import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.util.Bits;

/**
 * Dynamic store that stores strings.
 */
class DynamicArrayStore extends AbstractDynamicStore
{
    // store version, each store ends with this string (byte encoded)
    private static final String VERSION = "ArrayPropertyStore v0.9.9";

    public DynamicArrayStore( String fileName, Map<?,?> config, IdType idType )
    {
        super( fileName, config, idType );
    }

    public String getTypeAndVersionDescriptor()
    {
        return VERSION;
    }

    public static void createStore( String fileName, int blockSize,
            IdGeneratorFactory idGeneratorFactory )
    {
        createEmptyStore( fileName, blockSize, VERSION, idGeneratorFactory, IdType.ARRAY_BLOCK );
    }

    private Collection<DynamicRecord> allocateFromNumbers( long startBlock, Object array )
    {
        ShortArray type = ShortArray.typeOf( array );
        int arrayLength = Array.getLength( array );
        int requiredBits = type.calculateRequiredBitsForArray( array );
        int totalBits = requiredBits*arrayLength;
        int bytes = (totalBits-1)/8+1;
        int bitsUsedInLastByte = totalBits%8;
        bitsUsedInLastByte = bitsUsedInLastByte == 0 ? 8 : bitsUsedInLastByte;
        bytes += 3; // type + rest + requiredBits header. TODO no need to use full bytes
        Bits bits = Bits.bits( bytes );
        int length = arrayLength;
        for ( int i = length-1; i >= 0; i-- )
        {
            type.pushRight( Array.get( array, i ), bits, requiredBits ); 
        }
        bits.pushRight( (byte)requiredBits );
        bits.pushRight( (byte)bitsUsedInLastByte );
        bits.pushRight( (byte)type.intValue() );
        return allocateRecords( startBlock, bits.asLeftBytes() );
    }

    private Collection<DynamicRecord> allocateFromString( long startBlock,
        String[] array )
    {
        int size = 5;
        for ( String str : array )
        {
            size += 4 + str.length() * 2;
        }
        ByteBuffer buf = ByteBuffer.allocate( size );
        buf.put( (byte) PropertyType.STRING.intValue() );
        buf.putInt( array.length );
        for ( String str : array )
        {
            int length = str.length();
            char[] chars = new char[length];
            str.getChars( 0, length, chars, 0 );
            buf.putInt( length * 2 );
            for ( char c : chars )
            {
                buf.putChar( c );
            }
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
            buf.get(); // get rid of the type byte (which we've already read)
            String[] array = new String[buf.getInt()];
            for ( int i = 0; i < array.length; i++ )
            {
                int charLength = buf.getInt() / 2;
                char charBuffer[] = new char[charLength];
                for ( int j = 0; j < charLength; j++ )
                {
                    charBuffer[j] = buf.getChar();
                }
                array[i] = new String( charBuffer );
            }
            return array;
        }
        else
        {
            ShortArray type = ShortArray.typeOf( typeId );
            Bits bits = Bits.bitsFromBytesLeft( bArray );
            bits.pullLeftByte(); // type, we already got it 
            int bitsUsedInLastByte = bits.pullLeftByte();
            int requiredBits = bits.pullLeftByte();
            if ( requiredBits == 0 ) return type.createArray( 0 );
            int length = ((bArray.length-3)*8-(8-bitsUsedInLastByte))/requiredBits;
            Object result = type.createArray( length );
            for ( int i = 0; i < length; i++ )
            {
                type.pullLeft( bits, result, i, requiredBits );
            }
            return result;
        }
    }

    @Override
    protected boolean versionFound( String version )
    {
        if ( !version.startsWith( "ArrayPropertyStore" ) )
        {
            // non clean shutdown, need to do recover with right neo
            return false;
        }
//        if ( version.equals( "ArrayPropertyStore v0.9.3" ) )
//        {
//            rebuildIdGenerator();
//            closeIdGenerator();
//            return true;
//        }
        if ( version.equals( "ArrayPropertyStore v0.9.5" ) )
        {
            long blockSize = getBlockSize();
            // 0xFFFF + 13 for inUse,length,prev,next
            if ( blockSize > 0xFFFF + BLOCK_HEADER_SIZE )
            {
                throw new IllegalStoreVersionException( "Store version[" + version +
                        "] has " + (blockSize - BLOCK_HEADER_SIZE) + " block size " +
                        "(limit is " + 0xFFFF + ") and can not be upgraded to a newer version." );
            }
            return true;
        }
        throw new IllegalStoreVersionException( "Store version [" + version  + 
            "]. Please make sure you are not running old Neo4j kernel " + 
            " towards a store that has been created by newer version " + 
            " of Neo4j." );
    }
}