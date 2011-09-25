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

import org.neo4j.helpers.UTF8;
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

    public DynamicArrayStore( String fileName, Map<?,?> config, IdType idType )
    {
        super( fileName, config, idType );
    }

    @Override
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
        int size = 1+4;
        ByteBuffer buf = null; 
        if ( isAllLatin1( array ) )
        {   // LATIN-1
            for ( String string : array ) size += 4 + string.length();
            buf = ByteBuffer.allocate( size );
            // [eett,tttt] e=1
            buf.put( (byte)((1 << 6) | PropertyType.STRING.intValue()) );
            buf.putInt( array.length );
            for ( String str : array )
            {
                int length = str.length();
                buf.putInt( length );
                for ( int i = 0; i < length; i++ ) buf.put( (byte) str.charAt( i ) );
            }
        }
        else
        {   // UTF-8
            List<byte[]> stringsAsBytes = new ArrayList<byte[]>();
            for ( String string : array )
            {
                byte[] stringAsBytes = UTF8.encode( string );
                size += 4 + stringAsBytes.length;
                stringsAsBytes.add( stringAsBytes );
            }
            buf = ByteBuffer.allocate( size );
            // [eett,tttt] e=0
            buf.put( (byte)PropertyType.STRING.intValue() );
            buf.putInt( array.length );
            for ( byte[] stringAsBytes : stringsAsBytes )
            {
                buf.putInt( stringAsBytes.length );
                buf.put( stringAsBytes );
            }
        }
        return allocateRecords( startBlock, buf.array() );
    }

    private boolean isAllLatin1( String[] array )
    {
        for ( String string : array )
        {
            if ( !PropertyStore.isLatin1( string ) ) return false;
        }
        return true;
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
        byte typeId = (byte)(bArray[0] & 0x3F);
        if ( typeId == PropertyType.STRING.intValue() )
        {
            byte encoding = (byte)((bArray[0] & 0x30) >> 6);
            ByteBuffer buf = ByteBuffer.wrap( bArray );
            buf.get(); // get rid of the type byte (which we've already read)
            String[] array = new String[buf.getInt()];
            for ( int r = 0; r < array.length; r++ )
            {
                if ( encoding == 1 )
                {   // LATIN-1
                    char[] chars = new char[buf.getInt()];
                    for ( int i = 0; i < chars.length; i++ ) chars[i] = (char)buf.get();
                    array[r] = new String( chars );
                }    
                else
                {   // UTF-8
                    byte[] bytes = new byte[buf.getInt()];
                    buf.get( bytes );
                    array[r] = UTF8.decode( bytes );
                }
            }
            return array;
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