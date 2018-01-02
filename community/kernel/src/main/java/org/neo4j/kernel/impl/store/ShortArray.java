/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.store;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.Map;

import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.util.Bits;

public enum ShortArray
{
    BOOLEAN( PropertyType.BOOL, 1, Boolean.class, boolean.class )
    {
        @Override
        int getRequiredBits( Object array, int arrayLength )
        {
            return 1;
        }

        @Override
        public
        void writeAll( Object array, int length, int requiredBits, Bits result )
        {
            if ( isPrimitive( array ) )
            {
                for ( boolean value : (boolean[]) array )
                {
                    result.put( value ? 1 : 0, 1 );
                }
            } else
            {
                for ( boolean value : (Boolean[]) array )
                {
                    result.put( value ? 1 : 0, 1 );
                }
            }
        }

        @Override
        public
        Object createArray( int length, Bits bits, int requiredBits )
        {
            if ( length == 0 )
            {
                return EMPTY_BOOLEAN_ARRAY;
            }
            final boolean[] result = new boolean[length];
            for ( int i = 0; i < length; i++ )
            {
                result[i] = bits.getByte( requiredBits ) != 0;
            }
            return result;
        }

        @Override
        public Object createEmptyArray()
        {
            return EMPTY_BOOLEAN_ARRAY;
        }
    },
    BYTE( PropertyType.BYTE, 8, Byte.class, byte.class )
    {
        int getRequiredBits( byte value )
        {
            long mask = 1L << maxBits - 1;
            for ( int i = maxBits; i > 0; i--, mask >>= 1 )
            {
                if ( (mask & value) != 0 )
                {
                    return i;
                }
            }
            return 1;
        }

        @Override
        int getRequiredBits( Object array, int arrayLength )
        {
            int highest = 1;
            if ( isPrimitive( array ) )
            {
                for ( byte value : (byte[]) array )
                {
                    highest = Math.max( getRequiredBits( value ), highest );
                }
            } else
            {
                for ( byte value : (Byte[]) array )
                {
                    highest = Math.max( getRequiredBits( value ), highest );
                }
            }
            return highest;
        }

        @Override
        public
        void writeAll( Object array, int length, int requiredBits, Bits result )
        {
            if ( isPrimitive( array ) )
            {
                for ( byte b : (byte[]) array )
                {
                    result.put( b, requiredBits );
                }
            } else
            {
                for ( byte b : (Byte[]) array )
                {
                    result.put( b, requiredBits );
                }
            }
        }

        @Override
        public
        Object createArray( int length, Bits bits, int requiredBits )
        {
            if ( length == 0 )
            {
                return EMPTY_BYTE_ARRAY;
            }
            final byte[] result = new byte[length];
            for ( int i = 0; i < length; i++ )
            {
                result[i] = bits.getByte( requiredBits );
            }
            return result;
        }

        @Override
        public Object createEmptyArray()
        {
            return EMPTY_BYTE_ARRAY;
        }

    },
    SHORT( PropertyType.SHORT, 16, Short.class, short.class )
    {
        int getRequiredBits( short value )
        {
            long mask = 1L << maxBits - 1;
            for ( int i = maxBits; i > 0; i--, mask >>= 1 )
            {
                if ( (mask & value) != 0 )
                {
                    return i;
                }
            }
            return 1;
        }

        @Override
        int getRequiredBits( Object array, int arrayLength )
        {
            int highest = 1;
            if ( isPrimitive( array ) )
            {
                for ( short value : (short[]) array )
                {
                    highest = Math.max( getRequiredBits( value ), highest );
                }
            } else
            {
                for ( short value : (Short[]) array )
                {
                    highest = Math.max( getRequiredBits( value ), highest );
                }
            }
            return highest;
        }

        @Override
        public
        void writeAll( Object array, int length, int requiredBits, Bits result )
        {
            if ( isPrimitive( array ) )
            {
                for ( short value : (short[]) array )
                {
                    result.put( value, requiredBits );
                }
            } else
            {
                for ( short value : (Short[]) array )
                {
                    result.put( value, requiredBits );
                }
            }
        }

        @Override
        public
        Object createArray( int length, Bits bits, int requiredBits )
        {
            if ( length == 0 )
            {
                return EMPTY_SHORT_ARRAY;
            }
            final short[] result = new short[length];
            for ( int i = 0; i < length; i++ )
            {
                result[i] = bits.getShort( requiredBits );
            }
            return result;
        }

        @Override
        public Object createEmptyArray()
        {
            return EMPTY_SHORT_ARRAY;
        }
    },
    CHAR( PropertyType.CHAR, 16, Character.class , char.class)
    {
        int getRequiredBits( char value )
        {
            long mask = 1L << maxBits - 1;
            for ( int i = maxBits; i > 0; i--, mask >>= 1 )
            {
                if ( (mask & value) != 0 )
                {
                    return i;
                }
            }
            return 1;
        }

        @Override
        int getRequiredBits( Object array, int arrayLength )
        {
            int highest = 1;
            if ( isPrimitive( array ) )
            {
                for ( char value : (char[]) array )
                {
                    highest = Math.max( getRequiredBits( value ), highest );
                }
            } else
            {
                for ( char value : (Character[]) array )
                {
                    highest = Math.max( getRequiredBits( value ), highest );
                }
            }
            return highest;
        }

        @Override
        public
        void writeAll( Object array, int length, int requiredBits, Bits result )
        {
            if ( isPrimitive( array ) )
            {
                for ( char value : (char[]) array )
                {
                    result.put( value, requiredBits );
                }
            } else
            {
                for ( char value : (Character[]) array )
                {
                    result.put( value, requiredBits );
                }
            }
        }

        @Override
        public
        Object createArray( int length, Bits bits, int requiredBits )
        {
            if ( length == 0 )
            {
                return EMPTY_CHAR_ARRAY;
            }
            final char[] result = new char[length];
            for ( int i = 0; i < length; i++ )
            {
                result[i] = (char) bits.getShort( requiredBits );
            }
            return result;
        }

        @Override
        public Object createEmptyArray()
        {
            return EMPTY_CHAR_ARRAY;
        }
    },
    INT( PropertyType.INT, 32, Integer.class , int.class)
    {
        int getRequiredBits( int value )
        {
            long mask = 1L << maxBits - 1;
            for ( int i = maxBits; i > 0; i--, mask >>= 1 )
            {
                if ( (mask & value) != 0 )
                {
                    return i;
                }
            }
            return 1;
        }

        @Override
        int getRequiredBits( Object array, int arrayLength )
        {
            int highest = 1;
            if ( isPrimitive( array ) )
            {
                for ( int value : (int[]) array )
                {
                    highest = Math.max( getRequiredBits( value ), highest );
                }
            } else
            {
                for ( int value : (Integer[]) array )
                {
                    highest = Math.max( getRequiredBits( value ), highest );
                }
            }
            return highest;
        }

        @Override
        public
        void writeAll( Object array, int length, int requiredBits, Bits result )
        {
            if ( isPrimitive( array ) )
            {
                for ( int value : (int[]) array )
                {
                    result.put( value, requiredBits );
                }
            } else
            {
                for ( int value : (Integer[]) array )
                {
                    result.put( value, requiredBits );
                }
            }
        }

        @Override
        public
        Object createArray( int length, Bits bits, int requiredBits )
        {
            if ( length == 0 )
            {
                return EMPTY_INT_ARRAY;
            }
            final int[] result = new int[length];
            for ( int i = 0; i < length; i++ )
            {
                result[i] = bits.getInt( requiredBits );
            }
            return result;
        }

        @Override
        public Object createEmptyArray()
        {
            return EMPTY_INT_ARRAY;
        }
    },
    LONG( PropertyType.LONG, 64, Long.class , long.class)
    {
        @Override
        public int getRequiredBits( long value )
        {
            long mask = 1L << maxBits - 1;
            for ( int i = maxBits; i > 0; i--, mask >>= 1 )
            {
                if ( (mask & value) != 0 )
                {
                    return i;
                }
            }
            return 1;
        }


        @Override
        int getRequiredBits( Object array, int arrayLength )
        {
            int highest = 1;
            if ( isPrimitive( array ) )
            {
                for ( long value : (long[]) array )
                {
                    highest = Math.max( getRequiredBits( value ), highest );
                }
            } else
            {
                for ( long value : (Long[]) array )
                {
                    highest = Math.max( getRequiredBits( value ), highest );
                }
            }
            return highest;
        }

        @Override
        public
        void writeAll( Object array, int length, int requiredBits, Bits result )
        {
            if ( isPrimitive( array ) )
            {
                for ( long value : (long[]) array )
                {
                    result.put( value, requiredBits );
                }
            } else
            {
                for ( long value : (Long[]) array )
                {
                    result.put( value, requiredBits );
                }
            }
        }

        @Override
        public
        Object createArray( int length, Bits bits, int requiredBits )
        {
            if ( length == 0 )
            {
                return EMPTY_LONG_ARRAY;
            }
            final long[] result = new long[length];
            for ( int i = 0; i < length; i++ )
            {
                result[i] = bits.getLong( requiredBits );
            }
            return result;
        }

        @Override
        public Object createEmptyArray()
        {
            return EMPTY_LONG_ARRAY;
        }
    },
    FLOAT( PropertyType.FLOAT, 32, Float.class ,float.class)
    {
        int getRequiredBits( float value )
        {
            int v = Float.floatToIntBits( value );
            long mask = 1L << maxBits - 1;
            for ( int i = maxBits; i > 0; i--, mask >>= 1 )
            {
                if ( (mask & v) != 0 )
                {
                    return i;
                }
            }
            return 1;
        }

        @Override
        int getRequiredBits( Object array, int arrayLength )
        {
            int highest = 1;
            if ( isPrimitive( array ) )
            {
                for ( float value : (float[]) array )
                {
                    highest = Math.max( getRequiredBits( value ), highest );
                }
            } else
            {
                for ( float value : (Float[]) array )
                {
                    highest = Math.max( getRequiredBits( value ), highest );
                }
            }
            return highest;
        }

        @Override
        public
        void writeAll( Object array, int length, int requiredBits, Bits result )
        {
            if ( isPrimitive( array ) )
            {
                for ( float value : (float[]) array )
                {
                    result.put( Float.floatToIntBits( value ), requiredBits );
                }
            } else
            {
                for ( float value : (Float[]) array )
                {
                    result.put( Float.floatToIntBits( value ), requiredBits );
                }
            }
        }

        @Override
        public
        Object createArray( int length, Bits bits, int requiredBits )
        {
            if ( length == 0 )
            {
                return EMPTY_FLOAT_ARRAY;
            }
            final float[] result = new float[length];
            for ( int i = 0; i < length; i++ )
            {
                result[i] = Float.intBitsToFloat( bits.getInt( requiredBits ) );
            }
            return result;
        }

        @Override
        public Object createEmptyArray()
        {
            return EMPTY_FLOAT_ARRAY;
        }
    },
    DOUBLE( PropertyType.DOUBLE, 64, Double.class, double.class )
    {
        int getRequiredBits( double value )
        {
            long v = Double.doubleToLongBits( value );
            long mask = 1L << maxBits - 1;
            for ( int i = maxBits; i > 0; i--, mask >>= 1 )
            {
                if ( (mask & v) != 0 )
                {
                    return i;
                }
            }
            return 1;
        }

        @Override
        int getRequiredBits( Object array, int arrayLength )
        {
            int highest = 1;
            if ( isPrimitive( array ) )
            {
                for ( double value : (double[]) array )
                {
                    highest = Math.max( getRequiredBits( value ), highest );
                }
            } else
            {
                for ( double value : (Double[]) array )
                {
                    highest = Math.max( getRequiredBits( value ), highest );
                }
            }
            return highest;
        }

        @Override
        public
        void writeAll( Object array, int length, int requiredBits, Bits result )
        {
            if ( isPrimitive( array ) )
            {
                for ( double value : (double[]) array )
                {
                    result.put( Double.doubleToLongBits( value ), requiredBits );
                }
            } else
            {
                for ( double value : (Double[]) array )
                {
                    result.put( Double.doubleToLongBits( value ), requiredBits );
                }
            }
        }

        @Override
        public
        Object createArray( int length, Bits bits, int requiredBits )
        {
            if ( length == 0 )
            {
                return EMPTY_DOUBLE_ARRAY;
            }
            final double[] result = new double[length];
            for ( int i = 0; i < length; i++ )
            {
                result[i] = Double.longBitsToDouble( bits.getLong( requiredBits ) );
            }
            return result;
        }

        @Override
        public Object createEmptyArray()
        {
            return EMPTY_DOUBLE_ARRAY;
        }
    };
    public static final boolean[] EMPTY_BOOLEAN_ARRAY = new boolean[0];
    public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    public static final short[] EMPTY_SHORT_ARRAY = new short[0];
    public static final char[] EMPTY_CHAR_ARRAY = new char[0];
    public static final int[] EMPTY_INT_ARRAY = new int[0];
    public static final long[] EMPTY_LONG_ARRAY = new long[0];
    public static final float[] EMPTY_FLOAT_ARRAY = new float[0];
    public static final double[] EMPTY_DOUBLE_ARRAY = new double[0];

    private static boolean isPrimitive( Object array )
    {
        return array.getClass().getComponentType().isPrimitive();
    }

    private static final ShortArray[] TYPES = ShortArray.values();
    private static final Map<Class<?>, ShortArray> all = new IdentityHashMap<>( TYPES.length * 2 );

    static
    {
        for ( ShortArray shortArray : TYPES )
        {
            all.put( shortArray.primitiveClass, shortArray );
            all.put( shortArray.boxedClass, shortArray );
        }
    }

    final int maxBits;

    private final Class<?> boxedClass;
    private final Class<?> primitiveClass;
    private final PropertyType type;

    ShortArray( PropertyType type, int maxBits, Class<?> boxedClass, Class<?> primitiveClass )
    {
        this.type = type;
        this.maxBits = maxBits;
        this.boxedClass = boxedClass;
        this.primitiveClass = primitiveClass;
    }

    public int intValue()
    {
        return type.intValue();
    }

    public abstract Object createArray(int length, Bits bits, int requiredBits);

    public static boolean encode( int keyId, Object array,
                                  PropertyBlock target, int payloadSizeInBytes )
    {
        /*
         *  If the array is huge, we don't have to check anything else.
         *  So do the length check first.
         */
        int arrayLength = Array.getLength( array );
        if ( arrayLength > 63 )/*because we only use 6 bits for length*/
        {
            return false;
        }

        ShortArray type = typeOf( array );
        if ( type == null )
        {
            return false;
        }

        int requiredBits = type.calculateRequiredBitsForArray( array, arrayLength );
        if ( !willFit( requiredBits, arrayLength, payloadSizeInBytes ) )
        {
            // Too big array
            return false;
        }
        final int numberOfBytes = calculateNumberOfBlocksUsed( arrayLength, requiredBits ) * 8;
        if ( Bits.requiredLongs( numberOfBytes ) > PropertyType.getPayloadSizeLongs() )
        {
            return false;
        }
        Bits result = Bits.bits( numberOfBytes );
        // [][][    ,bbbb][bbll,llll][yyyy,tttt][kkkk,kkkk][kkkk,kkkk][kkkk,kkkk]
        writeHeader( keyId, type, arrayLength, requiredBits, result );
        type.writeAll( array, arrayLength, requiredBits, result );
        target.setValueBlocks( result.getLongs() );
        return true;
    }

    private static void writeHeader( int keyId, ShortArray type, int arrayLength, int requiredBits, Bits result )
    {
        result.put( keyId, 24 );
        result.put( PropertyType.SHORT_ARRAY.intValue(), 4 );
        result.put( type.type.intValue(), 4 );
        result.put( arrayLength, 6 );
        result.put( requiredBits, 6 );
    }

    public static Object decode( PropertyBlock block )
    {
        Bits bits = Bits.bitsFromLongs( Arrays.copyOf( block.getValueBlocks(), block.getValueBlocks().length) );
        return decode( bits );
    }

    public static Object decode( Bits bits )
    {
        // [][][    ,bbbb][bbll,llll][yyyy,tttt][kkkk,kkkk][kkkk,kkkk][kkkk,kkkk]
        bits.getInt( 24 ); // Get rid of key
        bits.getByte( 4 ); // Get rid of short array type
        int typeId = bits.getByte( 4 );
        int arrayLength = bits.getByte(6);
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
        return type == null? null : type.createArray(arrayLength, bits, requiredBits);
    }


    private static boolean willFit( int requiredBits, int arrayLength, int payloadSizeInBytes )
    {
        int totalBitsRequired = requiredBits*arrayLength;
        int maxBits = payloadSizeInBytes * 8 - 24 - 4 - 4 - 6 - 6;
        return totalBitsRequired <= maxBits;
    }

    public int calculateRequiredBitsForArray(Object array, int arrayLength)
    {
        if ( arrayLength == 0 )
        {
            return 0;
        }
        // return getRequiredBits(findBiggestValue(array, arrayLength));
        return getRequiredBits(array, arrayLength);
    }

    public int getRequiredBits( long value )
    {
        int highest = 1;
        long mask = 1;
        for ( int i = 1; i <= maxBits; i++, mask <<= 1 )
        {
            if ( (mask & value) != 0 )
            {
                highest = i;
            }
        }
        return highest;
    }

    abstract int getRequiredBits(Object array, int arrayLength);

    public static ShortArray typeOf( byte typeId )
    {
        return TYPES[typeId-1];
    }

    public static ShortArray typeOf( Object array )
    {
        return ShortArray.all.get(array.getClass().getComponentType());
    }

    public static int calculateNumberOfBlocksUsed( long firstBlock )
    {
        // inside the high 4B of the first block of a short array sits the header
        int highInt = (int) (firstBlock >>> 32);
        // bits 32-37 contains number of items (length)
        int arrayLength = highInt & 0b11_1111;
        highInt >>>= 6;
        // bits 38-43 contains number of requires bits per item
        int requiredBits = highInt & 0b11_1111;
        // no values can be represented by 0 bits, so we use that value for 64 instead
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

    public abstract void writeAll(Object array, int length, int requiredBits, Bits result);

    public Object createEmptyArray()
    {
        return Array.newInstance( primitiveClass, 0 );
    }
}
