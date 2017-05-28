/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.values;

import java.lang.reflect.Array;
import java.util.Comparator;

/**
 * Entry point to the values library.
 *
 * The values library centers around the Value class, which represents a value in Neo4j. Values can be correctly
 * checked for equality over different primitive representations, including consistent hashCodes and sorting.
 *
 * To create Values use the factory methods in the Values class.
 *
 * Values come in two major categories: Storable and Virtual. Storable values are valid values for
 * node, relationship and graph properties. Virtual values are not supported as property values, but might be created
 * and returned as part of cypher execution. These include Node, Relationship and Path.
 */
@SuppressWarnings( "WeakerAccess" )
public class Values
{
    private Values()
    {
    }

    interface ValueLoader<T>
    {
        T load() throws ValueLoadException;
    }

    public class ValueLoadException extends RuntimeException
    {
    }

    /**
     * Default value comparator. Will correctly compare all storable values and order the value groups according the
     * to comparability group. Virtual values are sorted in a random but deterministic fashion (by hashCode).
     */
    public static final ValueComparator VALUE_COMPARATOR =
            new ValueComparator(
                    ValueGroup::compareTo,
                    Comparator.comparingInt( VirtualValue::hashCode )
                );

    public static boolean isNumberValue( Object value )
    {
        return value instanceof NumberValue;
    }

    public static boolean isTextValue( Object value )
    {
        return value instanceof TextValue;
    }

    // DIRECT FACTORY METHODS

    public static final Value NO_VALUE = NoValue.NO_VALUE;

    public static Value stringValue( String value )
    {
        if ( value == null )
        {
            return NO_VALUE;
        }
        return new StringValue.Direct( value );
    }

    public static Value lazyStringValue( ValueLoader<String> producer )
    {
        return new StringValue.Lazy( producer );
    }

    public static Value lazyByteArray( ValueLoader<byte[]> producer )
    {
        return new ByteArray.Lazy( producer );
    }

    public static Value lazyShortArray( ValueLoader<short[]> producer )
    {
        return new ShortArray.Lazy( producer );
    }

    public static Value lazyIntArray( ValueLoader<int[]> producer )
    {
        return new IntArray.Lazy( producer );
    }

    public static Value lazyLongArray( ValueLoader<long[]> producer )
    {
        return new LongArray.Lazy( producer );
    }

    public static Value lazyFloatArray( ValueLoader<float[]> producer )
    {
        return new FloatArray.Lazy( producer );
    }

    public static Value lazyDoubleArray( ValueLoader<double[]> producer )
    {
        return new DoubleArray.Lazy( producer );
    }

    public static Value lazyCharArray( ValueLoader<char[]> producer )
    {
        return new CharArray.Lazy( producer );
    }

    public static Value lazyStringArray( ValueLoader<String[]> producer )
    {
        return new StringArray.Lazy( producer );
    }

    public static Value lazyBooleanArray( ValueLoader<boolean[]> producer )
    {
        return new BooleanArray.Lazy( producer );
    }

    public static Value numberValue( Number number )
    {
        if ( number instanceof Long )
        {
            return longValue( number.longValue() );
        }
        if ( number instanceof Integer )
        {
            return intValue( number.intValue() );
        }
        if ( number instanceof Double )
        {
            return doubleValue( number.doubleValue() );
        }
        if ( number instanceof Byte )
        {
            return byteValue( number.byteValue() );
        }
        if ( number instanceof Float )
        {
            return floatValue( number.floatValue() );
        }
        if ( number instanceof Short )
        {
            return shortValue( number.shortValue() );
        }
        if ( number == null )
        {
            return NO_VALUE;
        }

        throw new UnsupportedOperationException( "Unsupported type of Number " + number.toString() );
    }

    public static Value longValue( long value )
    {
        return new LongValue( value );
    }

    public static Value intValue( int value )
    {
        return new IntValue( value );
    }

    public static Value shortValue( short value )
    {
        return new ShortValue( value );
    }

    public static Value byteValue( byte value )
    {
        return new ByteValue( value );
    }

    public static Value booleanValue( boolean value )
    {
        return new BooleanValue( value );
    }

    public static Value charValue( char value )
    {
        return new CharValue( value );
    }

    public static Value doubleValue( double value )
    {
        return new DoubleValue( value );
    }

    public static Value floatValue( float value )
    {
        return new FloatValue( value );
    }

    public static Value stringArray( String[] value )
    {
        return new StringArray.Direct( value );
    }

    public static Value byteArray( byte[] value )
    {
        return new ByteArray.Direct( value );
    }

    public static Value longArray( long[] value )
    {
        return new LongArray.Direct( value );
    }

    public static Value intArray( int[] value )
    {
        return new IntArray.Direct( value );
    }

    public static Value doubleArray( double[] value )
    {
        return new DoubleArray.Direct( value );
    }

    public static Value floatArray( float[] value )
    {
        return new FloatArray.Direct( value );
    }

    public static Value booleanArray( boolean[] value )
    {
        return new BooleanArray.Direct( value );
    }

    public static Value charArray( char[] value )
    {
        return new CharArray.Direct( value );
    }

    public static Value shortArray( short[] value )
    {
        return new ShortArray.Direct( value );
    }

    // BOXED FACTORY METHODS

    /**
     * Generic value factory method.
     *
     * Beware, this method is intended for converting externally supplied values to the internal Value type, and to
     * make testing convenient. Passing a Value and in parameter should never be needed, and will throw an
     * UnsupportedOperationException.
     *
     * This method does defensive copying of arrays, while the explicit *Array() factory methods do not.
     *
     * @param value Object to convert to Value
     * @return the created Value
     */
    public static Value of( Object value )
    {
        if ( value instanceof String )
        {
            return stringValue( (String) value );
        }
        if ( value instanceof Object[] )
        {
            return arrayValue( (Object[]) value );
        }
        if ( value instanceof Long )
        {
            return longValue( (Long) value );
        }
        if ( value instanceof Integer )
        {
            return intValue( (Integer) value );
        }
        if ( value instanceof Boolean )
        {
            return booleanValue( (Boolean) value );
        }
        if ( value instanceof Double )
        {
            return doubleValue( (Double) value );
        }
        if ( value instanceof Float )
        {
            return floatValue( (Float) value );
        }
        if ( value instanceof Short )
        {
            return shortValue( (Short) value );
        }
        if ( value instanceof Byte )
        {
            return byteValue( (Byte) value );
        }
        if ( value instanceof Character )
        {
            return charValue( (Character) value );
        }
        if ( value instanceof byte[] )
        {
            return byteArray( ((byte[]) value).clone() );
        }
        if ( value instanceof long[] )
        {
            return longArray( ((long[]) value).clone() );
        }
        if ( value instanceof int[] )
        {
            return intArray( ((int[]) value).clone() );
        }
        if ( value instanceof double[] )
        {
            return doubleArray( ((double[]) value).clone() );
        }
        if ( value instanceof float[] )
        {
            return floatArray( ((float[]) value).clone() );
        }
        if ( value instanceof boolean[] )
        {
            return booleanArray( ((boolean[]) value).clone() );
        }
        if ( value instanceof char[] )
        {
            return charArray( ((char[]) value).clone() );
        }
        if ( value instanceof short[] )
        {
            return shortArray( ((short[]) value).clone() );
        }
        if ( value == null )
        {
            return NoValue.NO_VALUE;
        }
        if ( value instanceof Value )
        {
            throw new UnsupportedOperationException(
                    "Converting a Value to a Value using Values.of() is not supported." );
        }

        // otherwise fail
        throw new IllegalArgumentException(
                    String.format( "[%s:%s] is not a supported property value", value, value.getClass().getName() ) );
    }

    private static Value arrayValue( Object[] value )
    {
        if ( value instanceof String[] )
        {
            return stringArray( copy( value, new String[value.length] ) );
        }
        if ( value instanceof Byte[] )
        {
            return byteArray( copy( value, new byte[value.length] ) );
        }
        if ( value instanceof Long[] )
        {
            return longArray( copy( value, new long[value.length] ) );
        }
        if ( value instanceof Integer[] )
        {
            return intArray( copy( value, new int[value.length] ) );
        }
        if ( value instanceof Double[] )
        {
            return doubleArray( copy( value, new double[value.length] ) );
        }
        if ( value instanceof Float[] )
        {
            return floatArray( copy( value, new float[value.length] ) );
        }
        if ( value instanceof Boolean[] )
        {
            return booleanArray( copy( value, new boolean[value.length] ) );
        }
        if ( value instanceof Character[] )
        {
            return charArray( copy( value, new char[value.length] ) );
        }
        if ( value instanceof Short[] )
        {
            return shortArray( copy( value, new short[value.length] ) );
        }
        throw new IllegalArgumentException(
                String.format( "%s[] is not a supported property value type",
                               value.getClass().getComponentType().getName() ) );
    }

    private static <T> T copy( Object[] value, T target )
    {
        for ( int i = 0; i < value.length; i++ )
        {
            if ( value[i] == null )
            {
                throw new IllegalArgumentException( "Property array value elements may not be null." );
            }
            Array.set( target, i, value[i] );
        }
        return target;
    }
}
