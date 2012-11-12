/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
import java.util.Arrays;

import org.neo4j.kernel.impl.util.Bits;

public enum ShortArray
{
    BOOLEAN( PropertyType.BOOL, 1, Boolean.class )
    {
        @Override
        int getRequiredBits( Object value )
        {
            return 1;
        }

        @Override
        void put( Object value, Bits bytes, int requiredBits )
        {
            bytes.put( ((Boolean)value).booleanValue() ? 1 : 0, 1 );
        }

        @Override
        Object createArray( int ofLength )
        {
            return new boolean[ofLength];
        }

        @Override
        void get( Object array, int position, Bits bits, int requiredBits )
        {
            Array.setBoolean( array, position, bits.getByte( requiredBits ) != 0 );
        }
    },
    BYTE( PropertyType.BYTE, 8, Byte.class )
    {
        @Override
        int getRequiredBits( Object value )
        {
            byte v = ((Number)value).byteValue();
            int highest = 0;
            long mask = 1;
            for ( int i = 1; i <= maxBits; i++, mask <<= 1 )
            {
                if ( (mask&v) != 0 )
                {
                    highest = i;
                }
            }
            return highest;
        }

        @Override
        void put( Object value, Bits bytes, int requiredBits )
        {
            bytes.put( ((Byte)value).byteValue(), requiredBits );
        }

        @Override
        Object createArray( int ofLength )
        {
            return new byte[ofLength];
        }

        @Override
        void get( Object array, int position, Bits bits, int requiredBits )
        {
            Array.setByte( array, position, bits.getByte( requiredBits ) );
        }
    },
    SHORT( PropertyType.SHORT, 16, Short.class )
    {
        @Override
        int getRequiredBits( Object value )
        {
            short v = ((Number)value).shortValue();
            int highest = 0;
            long mask = 1;
            for ( int i = 1; i <= maxBits; i++, mask <<= 1 )
            {
                if ( (mask&v) != 0 )
                {
                    highest = i;
                }
            }
            return highest;
        }

        @Override
        void put( Object value, Bits bytes, int requiredBits )
        {
            bytes.put( ((Short)value).shortValue(), requiredBits );
        }

        @Override
        Object createArray( int ofLength )
        {
            return new short[ofLength];
        }

        @Override
        void get( Object array, int position, Bits bits, int requiredBits )
        {
            Array.setShort( array, position, bits.getShort( requiredBits ) );
        }
    },
    CHAR( PropertyType.CHAR, 16, Character.class )
    {
        @Override
        int getRequiredBits( Object value )
        {
            char v = ((Character)value).charValue();
            int highest = 0;
            long mask = 1;
            for ( int i = 1; i <= maxBits; i++, mask <<= 1 )
            {
                if ( (mask&v) != 0 )
                {
                    highest = i;
                }
            }
            return highest;
        }

        @Override
        void put( Object value, Bits bytes, int requiredBits )
        {
            bytes.put( ((Character)value).charValue(), requiredBits );
        }

        @Override
        Object createArray( int ofLength )
        {
            return new char[ofLength];
        }

        @Override
        void get( Object array, int position, Bits bits, int requiredBits )
        {
            Array.setChar( array, position, (char)bits.getShort( requiredBits ) );
        }
    },
    INT( PropertyType.INT, 32, Integer.class )
    {
        @Override
        int getRequiredBits( Object value )
        {
            int v = ((Number)value).intValue();
            int highest = 0;
            long mask = 1;
            for ( int i = 1; i <= maxBits; i++, mask <<= 1 )
            {
                if ( (mask&v) != 0 )
                {
                    highest = i;
                }
            }
            return highest;
        }

        @Override
        void put( Object value, Bits bytes, int requiredBits )
        {
            bytes.put( ((Integer)value).intValue(), requiredBits );
        }

        @Override
        Object createArray( int ofLength )
        {
            return new int[ofLength];
        }

        @Override
        void get( Object array, int position, Bits bits, int requiredBits )
        {
            Array.setInt( array, position, bits.getInt( requiredBits ) );
        }
    },
    LONG( PropertyType.LONG, 64, Long.class )
    {
        @Override
        int getRequiredBits( Object value )
        {
            long v = ((Number)value).longValue();
            int highest = 0;
            long mask = 1;
            for ( int i = 1; i <= maxBits; i++, mask <<= 1 )
            {
                if ( (mask&v) != 0 )
                {
                    highest = i;
                }
            }
            return highest;
        }

        @Override
        void put( Object value, Bits bytes, int requiredBits )
        {
            bytes.put( ((Long)value).longValue(), requiredBits );
        }

        @Override
        Object createArray( int ofLength )
        {
            return new long[ofLength];
        }

        @Override
        void get( Object array, int position, Bits bits, int requiredBits )
        {
            Array.setLong( array, position, bits.getLong( requiredBits ) );
        }
    },
    FLOAT( PropertyType.FLOAT, 32, Float.class )
    {
        @Override
        int getRequiredBits( Object value )
        {
            int v = Float.floatToIntBits( ((Number)value).floatValue() );
            int highest = 0;
            long mask = 1;
            for ( int i = 1; i <= maxBits; i++, mask <<= 1 )
            {
                if ( (mask&v) != 0 )
                {
                    highest = i;
                }
            }
            return highest;
        }

        @Override
        void put( Object value, Bits bytes, int requiredBits )
        {
            bytes.put( Float.floatToIntBits( ((Float)value).floatValue() ), requiredBits );
        }

        @Override
        Object createArray( int ofLength )
        {
            return new float[ofLength];
        }

        @Override
        void get( Object array, int position, Bits bits, int requiredBits )
        {
            int value = bits.getInt( requiredBits );
            Array.setFloat( array, position, Float.intBitsToFloat( value ) );
        }
    },
    DOUBLE( PropertyType.DOUBLE, 64, Double.class )
    {
        @Override
        int getRequiredBits( Object value )
        {
            long v = Double.doubleToLongBits( ((Number)value).doubleValue() );
            int highest = 0;
            long mask = 1;
            for ( int i = 1; i <= maxBits; i++, mask <<= 1 )
            {
                if ( (mask&v) != 0 )
                {
                    highest = i;
                }
            }
            return highest;
        }

        @Override
        void put( Object value, Bits bytes, int requiredBits )
        {
            bytes.put( Double.doubleToLongBits( ((Double)value).doubleValue() ), requiredBits );
        }

        @Override
        Object createArray( int ofLength )
        {
            return new double[ofLength];
        }

        @Override
        void get( Object array, int position, Bits bits, int requiredBits )
        {
            long value = bits.getLong( requiredBits );
            Array.setDouble( array, position, Double.longBitsToDouble( value ) );
        }
    };

    final int maxBits;

    private final Class<?> boxedClass;
    private final PropertyType type;

    private ShortArray( PropertyType type, int maxBits, Class<?> boxedClass )
    {
        this.type = type;
        this.maxBits = maxBits;
        this.boxedClass = boxedClass;
    }

    public int intValue()
    {
        return type.intValue();
    }

    abstract int getRequiredBits( Object value );

    abstract void put( Object value, Bits bits, int requiredBits );

    abstract void get( Object array, int position, Bits bits, int requiredBits );

    abstract Object createArray( int ofLength );

    boolean matches( Class<?> cls )
    {
        return boxedClass.equals( cls );
    }

    public static boolean encode( int keyId, Object array,
            PropertyBlock target, int payloadSizeInBytes )
    {
        ShortArray type = typeOf( array );
        if ( type == null )
        {
            return false;
        }

        /*
         *  If the array is huge, we don't have to check anything else.
         *  So do the length check first.
         */
        int arrayLength = Array.getLength( array );
        if ( arrayLength > 63 )/*because we only use 6 bits for length*/
        {
            return false;
        }
        int requiredBits = type.calculateRequiredBitsForArray( array );
        if ( !willFit( requiredBits, arrayLength, payloadSizeInBytes ) )
        {
            // Too big array
            return false;
        }
        Bits result = Bits.bits( calculateNumberOfBlocksUsed( arrayLength, requiredBits )*8 );
        if ( result.getLongs().length > PropertyType.getPayloadSizeLongs() )
        {
            return false;
        }
        // [][][    ,bbbb][bbll,llll][yyyy,tttt][kkkk,kkkk][kkkk,kkkk][kkkk,kkkk]
        result.put( keyId, 24 );
        result.put( PropertyType.SHORT_ARRAY.intValue(), 4 );
        result.put( type.type.intValue(), 4 );
        result.put( arrayLength, 6 );
        result.put( requiredBits, 6 );

        for ( int i = 0; i < arrayLength; i++ )
        {
            type.put( Array.get( array, i ), result, requiredBits );
        }
        target.setValueBlocks( result.getLongs() );
        return true;
    }

    public static Object decode( PropertyBlock block )
    {
        Bits bits = Bits.bitsFromLongs( Arrays.copyOf( block.getValueBlocks(), block.getValueBlocks().length ) );
        // [][][    ,bbbb][bbll,llll][yyyy,tttt][kkkk,kkkk][kkkk,kkkk][kkkk,kkkk]
        bits.getInt( 24 ); // Get rid of key
        bits.getByte( 4 ); // Get rid of short array type
        int typeId = bits.getByte( 4 );
        int arrayLength = bits.getByte( 6 );
        int requiredBits = bits.getByte( 6 );
        /*
         * So, it can be the case that values require 64 bits to store. However, you cannot encode this
         * value with 6 bits. calculateRequiredBitsForArray never returns 0, because even for an array of
         * all 0s one bit is required for every value. So when writing, we let it overflow and write out
         * 0. When we are reading back, we just have to make sure that reading in 0 means 64.
         */
        if ( requiredBits == 0 )
        {
            requiredBits = 64;
        }
        ShortArray type = typeOf( (byte)typeId );
        Object array = type.createArray( arrayLength );
        for ( int i = 0; i < arrayLength; i++ )
        {
            type.get( array, i, bits, requiredBits );
        }
        return array;
    }

    private static boolean willFit( int requiredBits, int arrayLength, int payloadSizeInBytes )
    {
        int totalBitsRequired = requiredBits*arrayLength;
        int maxBits = payloadSizeInBytes * 8 - 24 - 4 - 4 - 6 - 6;
        return totalBitsRequired <= maxBits;
    }

    public int calculateRequiredBitsForArray( Object array )
    {
        int arrayLength = Array.getLength( array );
        if ( arrayLength == 0 )
        {
            return 0;
        }
        int highest = 1;
        for ( int i = 0; i < arrayLength; i++ )
        {
            Object value = Array.get( array, i );
            highest = Math.max( highest, getRequiredBits( value ) );
        }
        return highest;
    }

    public static ShortArray typeOf( byte typeId )
    {
        return ShortArray.values()[typeId-1];
    }

    public static ShortArray typeOf( Object array )
    {
        Class<?> componentType = array.getClass().getComponentType();
        if ( componentType.isPrimitive() )
        {
            String name = componentType.getSimpleName();
            return valueOf( name.toUpperCase() );
        }
        else
        {
            for ( ShortArray type : values() )
            {
                if ( type.matches( componentType ) )
                {
                    return type;
                }
            }
        }
        return null;
    }

    public static int calculateNumberOfBlocksUsed( long firstBlock )
    {
        Bits bits = Bits.bitsFromLongs( new long[] {firstBlock} );
        // bbbb][bbll,llll][yyyy,tttt][kkkk,kkkk][kkkk,kkkk][kkkk,kkkk]
        bits.getInt( 24 ); // Get rid of key
        bits.getByte( 4 ); // Get rid of short array type
        bits.getByte( 4 ); // Get rid of the type
        int arrayLength = bits.getByte( 6 );
        int requiredBits = bits.getByte( 6 );
        if ( requiredBits == 0 )
        {
            requiredBits = 64;
        }
        return calculateNumberOfBlocksUsed( arrayLength, requiredBits );
    }

    public static int calculateNumberOfBlocksUsed( int arrayLength, int requiredBits )
    {
        int bitsForItems = arrayLength*requiredBits;
        /*
         * Key, Property Type (ARRAY), Array Type, Array Length, Bits Per Member, Data
         */
        int totalBits = 24 + 4 + 4 + 6 + 6 + bitsForItems;
        int result = ( totalBits - 1 ) / 64 + 1;
        return result;
    }
}
