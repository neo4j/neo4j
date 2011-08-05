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
        void apply( Object value, Bits bytes )
        {
            bytes.or( ((Boolean)value) == Boolean.TRUE ? 1 : 0 );
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
        void apply( Object value, Bits bytes )
        {
            bytes.or( ((Byte)value).byteValue() );
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
        void apply( Object value, Bits bytes )
        {
            bytes.or( ((Short)value).shortValue() );
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
        void apply( Object value, Bits bytes )
        {
            bytes.or( ((Character)value).charValue() );
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
        void apply( Object value, Bits bytes )
        {
            bytes.or( ((Integer)value).intValue() );
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
        void apply( Object value, Bits bytes )
        {
            bytes.or( ((Long)value).longValue() );
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
        void apply( Object value, Bits bytes )
        {
            bytes.or( Float.floatToIntBits( ((Float)value).floatValue() ) );
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
        void apply( Object value, Bits bytes )
        {
            bytes.or( Double.doubleToLongBits( ((Double)value).doubleValue() ) );
        }
    };
    
    private static final int HEADER_SIZE = 5;
    
    final int maxBits;

    private ShortArray( int maxBits )
    {
        this.maxBits = maxBits;
    }
    
    abstract int getRequiredBits( Object value );
    
    abstract void apply( Object value, Bits bytes );
    
    public static void main( String[] args )
    {
        System.out.println( encode( new boolean[] { false, false, true, true,false, false, true, true,false, false, true, true,false, false, true, true}, null, 16 ) );
    }
    
    public static boolean encode( Object array, PropertyRecord record, int payloadSizeInBytes )
    {
        ShortArray type = typeOf( array );
        int requiredBits = type.calculateRequiredBitsForArray( array );
        int arrayLength = Array.getLength( array );
        if ( !willFit( requiredBits, arrayLength, payloadSizeInBytes ) )
        {
            // Too big array
            return false;
        }
        
        Bits result = new Bits( payloadSizeInBytes );
        for ( int i = 0; i < arrayLength; i++ )
        {
            type.apply( Array.get( array, i ), result );
            result.shiftLeft( requiredBits );
        }
        long[] longs = result.getLongs();
        // Apply the header all the way to the left
        longs[0] |= ((long)requiredBits) << ((payloadSizeInBytes*8)-HEADER_SIZE);
        // TODO Set it in propertyrecord
        System.out.println( new Bits( longs ) );
        return true;
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
        return valueOf( array.getClass().getComponentType().getSimpleName().toUpperCase() );
    }
}
