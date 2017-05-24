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
import java.util.concurrent.Callable;

@SuppressWarnings( "WeakerAccess" )
public class Values
{
    private Values()
    {
    }

    // DIRECT FACTORY METHODS

    public static Value noValue()
    {
        return NoValue.NO_VALUE;
    }

    public static Value stringValue( String value )
    {
        return new DirectString( value );
    }

    public static Value lazyStringValue( Callable<String> producer )
    {
        return new LazyString( producer );
    }

    public static Value lazyByteArray( Callable<byte[]> producer )
    {
        return new LazyByteArray( producer );
    }

    public static Value lazyShortArray( Callable<short[]> producer )
    {
        return new LazyShortArray( producer );
    }

    public static Value lazyIntArray( Callable<int[]> producer )
    {
        return new LazyIntArray( producer );
    }

    public static Value lazyLongArray( Callable<long[]> producer )
    {
        return new LazyLongArray( producer );
    }

    public static Value lazyFloatArray( Callable<float[]> producer )
    {
        return new LazyFloatArray( producer );
    }

    public static Value lazyDoubleArray( Callable<double[]> producer )
    {
        return new LazyDoubleArray( producer );
    }

    public static Value lazyCharArray( Callable<char[]> producer )
    {
        return new LazyCharArray( producer );
    }

    public static Value lazyStringArray( Callable<String[]> producer )
    {
        return new LazyStringArray( producer );
    }

    public static Value lazyBooleanArray( Callable<boolean[]> producer )
    {
        return new LazyBooleanArray( producer );
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

        throw new UnsupportedOperationException( "Unsupported type of Number " + number.toString() );
    }

    public static Value longValue( long value )
    {
        return new DirectLong( value );
    }

    public static Value intValue( int value )
    {
        return new DirectInt( value );
    }

    public static Value shortValue( short value )
    {
        return new DirectShort( value );
    }

    public static Value byteValue( byte value )
    {
        return new DirectByte( value );
    }

    public static Value booleanValue( boolean value )
    {
        return new DirectBoolean( value );
    }

    public static Value charValue( char value )
    {
        return new DirectChar( value );
    }

    public static Value doubleValue( double value )
    {
        return new DirectDouble( value );
    }

    public static Value floatValue( float value )
    {
        return new DirectFloat( value );
    }

    public static Value stringArray( String[] value )
    {
        return new DirectStringArray( value );
    }

    public static Value byteArray( byte[] value )
    {
        return new DirectByteArray( value );
    }

    public static Value longArray( long[] value )
    {
        return new DirectLongArray( value );
    }

    public static Value intArray( int[] value )
    {
        return new DirectIntArray( value );
    }

    public static Value doubleArray( double[] value )
    {
        return new DirectDoubleArray( value );
    }

    public static Value floatArray( float[] value )
    {
        return new DirectFloatArray( value );
    }

    public static Value booleanArray( boolean[] value )
    {
        return new DirectBooleanArray( value );
    }

    public static Value charArray( char[] value )
    {
        return new DirectCharArray( value );
    }

    public static Value shortArray( short[] value )
    {
        return new DirectShortArray( value );
    }

    // BOXED FACTORY METHODS

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
