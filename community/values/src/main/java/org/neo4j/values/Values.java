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

class Values
{
    private Values()
    {
    }

    // DIRECT FACTORY METHODS

    public static Value stringValue( String value )
    {
        return new StringValue( value );
    }

    public static Value lazyStringValue( Callable<String> producer )
    {
        return new LazyStringValue( producer );
    }

    public static Value lazyArrayValue( Callable<Object> producer )
    {
        return new LazyArrayValue( producer );
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

    public static Value stringArrayValue( String[] value )
    {
        return new StringArrayValue( value );
    }

    public static Value byteArrayValue( byte[] value )
    {
        return new ByteArrayValue( value );
    }

    public static Value longArrayValue( long[] value )
    {
        return new LongArrayValue( value );
    }

    public static Value intArrayValue( int[] value )
    {
        return new IntArrayValue( value );
    }

    public static Value doubleArrayValue( double[] value )
    {
        return new DoubleArrayValue( value );
    }

    public static Value floatArrayValue( float[] value )
    {
        return new FloatArrayValue( value );
    }

    public static Value booleanArrayValue( boolean[] value )
    {
        return new BooleanArrayValue( value );
    }

    public static Value charArrayValue( char[] value )
    {
        return new CharArrayValue( value );
    }

    public static Value shortArrayValue( short[] value )
    {
        return new ShortArrayValue( value );
    }

    // BOXED FACTORY METHODS

    static Value of( Object value )
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
            return byteArrayValue( ((byte[]) value).clone() );
        }
        if ( value instanceof long[] )
        {
            return longArrayValue( ((long[]) value).clone() );
        }
        if ( value instanceof int[] )
        {
            return intArrayValue( ((int[]) value).clone() );
        }
        if ( value instanceof double[] )
        {
            return doubleArrayValue( ((double[]) value).clone() );
        }
        if ( value instanceof float[] )
        {
            return floatArrayValue( ((float[]) value).clone() );
        }
        if ( value instanceof boolean[] )
        {
            return booleanArrayValue( ((boolean[]) value).clone() );
        }
        if ( value instanceof char[] )
        {
            return charArrayValue( ((char[]) value).clone() );
        }
        if ( value instanceof short[] )
        {
            return shortArrayValue( ((short[]) value).clone() );
        }
        // otherwise fail
        if ( value == null )
        {
            throw new IllegalArgumentException( "[null] is not a supported property value" );
        }
        else
        {
            throw new IllegalArgumentException(
                    String.format( "[%s:%s] is not a supported property value", value, value.getClass().getName() ) );
        }
    }

    private static Value arrayValue( Object[] value )
    {
        if ( value instanceof String[] )
        {
            return stringArrayValue( copy( value, new String[value.length] ) );
        }
        if ( value instanceof Byte[] )
        {
            return byteArrayValue( copy( value, new byte[value.length] ) );
        }
        if ( value instanceof Long[] )
        {
            return longArrayValue( copy( value, new long[value.length] ) );
        }
        if ( value instanceof Integer[] )
        {
            return intArrayValue( copy( value, new int[value.length] ) );
        }
        if ( value instanceof Double[] )
        {
            return doubleArrayValue( copy( value, new double[value.length] ) );
        }
        if ( value instanceof Float[] )
        {
            return floatArrayValue( copy( value, new float[value.length] ) );
        }
        if ( value instanceof Boolean[] )
        {
            return booleanArrayValue( copy( value, new boolean[value.length] ) );
        }
        if ( value instanceof Character[] )
        {
            return charArrayValue( copy( value, new char[value.length] ) );
        }
        if ( value instanceof Short[] )
        {
            return shortArrayValue( copy( value, new short[value.length] ) );
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
