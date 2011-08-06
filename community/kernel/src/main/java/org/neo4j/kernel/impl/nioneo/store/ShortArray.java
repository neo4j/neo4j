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

import org.neo4j.helpers.Pair;
import org.neo4j.kernel.impl.util.Bits;

public enum ShortArray
{
    BOOLEAN( 1 )
    {
        @Override
        int getRequiredBits( Object value )
        {
            return 1;
        }

        @Override
        void push( Object value, Bits bytes, long mask )
        {
            bytes.or( ((Boolean)value).booleanValue() ? (byte)1 : (byte)0, mask );
        }
        
        @Override
        Object createArray( int ofLength )
        {
            return new boolean[ofLength];
        }

        @Override
        void pull( Bits bits, Object array, int position, long mask )
        {
            Array.setBoolean( array, position, bits.getByte( (byte) mask ) != 0 );
        }
    },
    BYTE( 8 )
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
        void push( Object value, Bits bytes, long mask )
        {
            bytes.or( ((Byte)value).byteValue(), mask );
        }
        
        @Override
        Object createArray( int ofLength )
        {
            return new byte[ofLength];
        }

        @Override
        void pull( Bits bits, Object array, int position, long mask )
        {
            byte value = bits.getByte( (byte) mask );
            Array.setByte( array, position, value );
        }
    },
    SHORT( 16 )
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
        void push( Object value, Bits bytes, long mask )
        {
            bytes.or( ((Short)value).shortValue(), mask );
        }

        @Override
        Object createArray( int ofLength )
        {
            return new short[ofLength];
        }

        @Override
        void pull( Bits bits, Object array, int position, long mask )
        {
            short value = bits.getShort( (short) mask );
            Array.setShort( array, position, value );
        }
    },
    CHAR( 16 )
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
        void push( Object value, Bits bytes, long mask )
        {
            bytes.or( ((Character)value).charValue(), mask );
        }

        @Override
        Object createArray( int ofLength )
        {
            return new char[ofLength];
        }

        @Override
        void pull( Bits bits, Object array, int position, long mask )
        {
            int value = bits.getInt( (int) mask );
            Array.setInt( array, position, value );
        }
    },
    INT( 32 )
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
        void push( Object value, Bits bytes, long mask )
        {
            bytes.or( ((Integer)value).intValue(), mask );
        }

        @Override
        Object createArray( int ofLength )
        {
            return new int[ofLength];
        }

        @Override
        void pull( Bits bits, Object array, int position, long mask )
        {
            int value = bits.getInt( (int) mask );
            Array.setInt( array, position, value );
        }
    },
    LONG( 64 )
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
        void push( Object value, Bits bytes, long mask )
        {
            bytes.or( ((Long)value).longValue(), mask );
        }

        @Override
        Object createArray( int ofLength )
        {
            return new long[ofLength];
        }

        @Override
        void pull( Bits bits, Object array, int position, long mask )
        {
            long value = bits.getLong( mask );
            Array.setLong( array, position, value );
        }
    },
    FLOAT( 32 )
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
        void push( Object value, Bits bytes, long mask )
        {
            bytes.or( Float.floatToIntBits( ((Float)value).floatValue() ), mask );
        }

        @Override
        Object createArray( int ofLength )
        {
            return new float[ofLength];
        }

        @Override
        void pull( Bits bits, Object array, int position, long mask )
        {
            int value = bits.getInt( (int) mask );
            Array.setFloat( array, position, Float.intBitsToFloat( value ) );
        }
    },
    DOUBLE( 64 )
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
        void push( Object value, Bits bytes, long mask )
        {
            bytes.or( Double.doubleToLongBits( ((Double)value).doubleValue() ), mask );
        }

        @Override
        Object createArray( int ofLength )
        {
            return new double[ofLength];
        }

        @Override
        void pull( Bits bits, Object array, int position, long mask )
        {
            long value = bits.getLong( mask );
            Array.setDouble( array, position, Double.longBitsToDouble( value ) );
        }
    };
    
    private static final int HEADER_SIZE = 5;
    
    final int maxBits;

    private ShortArray( int maxBits )
    {
        this.maxBits = maxBits;
    }
    
    abstract int getRequiredBits( Object value );
    
    abstract void push( Object value, Bits bits, long mask );
    
    abstract Object createArray( int ofLength );
    
    abstract void pull( Bits bits, Object array, int position, long mask );
    
    public static Pair<long[], Integer> encode( Object array, int payloadSizeInBytes )
    {
        ShortArray type = typeOf( array );
        int requiredBits = type.calculateRequiredBitsForArray( array );
        int arrayLength = Array.getLength( array );
        if ( arrayLength > 32 || !willFit( requiredBits, arrayLength, payloadSizeInBytes ) )
        {
            // Too big array
            return null;
        }
        
        Bits result = new Bits( payloadSizeInBytes );
        long mask = Bits.rightOverspillMask( requiredBits );
        for ( int i = 0; i < arrayLength; i++ )
        {
            if ( i > 0 )
            {
                result.shiftLeft( requiredBits );
            }
            type.push( Array.get( array, i ), result, mask );
        }
        long[] longs = result.getLongs();
        // Apply the header all the way to the left
        longs[0] |= ((long)requiredBits) << (64-HEADER_SIZE);
        // TODO Set it in propertyrecord
        int header = (type.ordinal()<<5) | (arrayLength);
        return Pair.of( longs, header );
    }
    
    public static Object decode( Pair<long[], Integer> data )
    {
        int typeId = (data.other().intValue() & 0xE0) >> 5;
        ShortArray type = values()[typeId];
        int arrayLength = data.other().byteValue() & 0x1F;
        Object array = type.createArray( arrayLength );
        
        long[] longs = data.first();
        int requiredBits = (int) ((longs[0] & 0xF800000000000000L) >>> (64-HEADER_SIZE));
        Bits bits = new Bits( longs );
        long mask = Bits.rightOverspillMask( requiredBits );
        for ( int i = arrayLength-1; i >= 0; i-- )
        {
            type.pull( bits, array, i, mask );
            bits.shiftRight( requiredBits );
        }
        return array;
    }
    
    private static boolean willFit( int requiredBits, int arrayLength, int payloadSizeInBytes )
    {
        int totalBitsRequired = requiredBits*arrayLength;
        int maxBits = payloadSizeInBytes*8-5;
        return totalBitsRequired <= maxBits;
    }

    public int calculateRequiredBitsForArray( Object array )
    {
        int arrayLength = Array.getLength( array );
        int highest = 0;
        for ( int i = 0; i < arrayLength; i++ )
        {
            Object value = Array.get( array, i );
            highest = Math.max( highest, getRequiredBits( value ) );
        }
        return highest;
    }

    public static ShortArray typeOf( Object array )
    {
        Class<?> componentType = array.getClass().getComponentType();
        if ( componentType.equals( String.class ) )
        {
            throw new IllegalArgumentException( "String arrays not allowed" );
        }
        String name = array.getClass().getComponentType().getSimpleName();
        return valueOf( name.toUpperCase() );
    }
}
