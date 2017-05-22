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

import java.util.concurrent.Callable;

class LazyArrayValue extends LazyValue<Object>
{
    /*
     * Access to this field needs synchronization, since it must be safe for use from multiple threads.
     * The synchronization of this field is carefully designed to be implicit.
     *
     *
     * assuming: produceValue() is called under synchronization - this is where this field is written.
     *           produceValue() is called *before* assigning the volatile LazyProperty.value field
     *                             (still under synchronization)
     * assuming: value member field is volatile, so accessing it implies the required read barrier.
     *           type doesn't need to be volatile since any call path to it first reads value,
     *           it's ALWAYS written before value, implying write barrier, and read after value, implying read barrier.
     */
    private Type type;

    LazyArrayValue( final Callable<?> producer )
    {
        super( producer );
    }

    @Override
    protected Object produceValue()
    {
        // this method is called under synchronization, before assigning LazyProperty.value ...
        Object value = super.produceValue();
        this.type = Type.from( value ); // ... so assigning type is safe
        return value;
    }

    @Override
    public boolean equals( Object other )
    {
        return other != null && other instanceof Value && equals( (Value) other );
    }

    @Override
    public boolean equals( Value value )
    {
        Object myValue = value(); // value() accesses LazyProperty.value, implying a read barrier ...
        return type.equals( myValue, value ); // ... so accessing type is safe
    }

    @Override
    boolean equals( byte[] x )
    {
        return PrimitiveArrayValues.equalsObject( x, value() );
    }

    @Override
    boolean equals( short[] x )
    {
        return PrimitiveArrayValues.equalsObject( x, value() );
    }

    @Override
    boolean equals( int[] x )
    {
        return PrimitiveArrayValues.equalsObject( x, value() );
    }

    @Override
    boolean equals( long[] x )
    {
        return PrimitiveArrayValues.equalsObject( x, value() );
    }

    @Override
    boolean equals( float[] x )
    {
        return PrimitiveArrayValues.equalsObject( x, value() );
    }

    @Override
    boolean equals( double[] x )
    {
        return PrimitiveArrayValues.equalsObject( x, value() );
    }

    @Override
    boolean equals( boolean x )
    {
        return false;
    }

    @Override
    boolean equals( char x )
    {
        return false;
    }

    @Override
    boolean equals( String x )
    {
        return false;
    }

    @Override
    boolean equals( char[] x )
    {
        return PrimitiveArrayValues.equalsObject( x, value() );
    }

    @Override
    boolean equals( String[] x )
    {
        return PrimitiveArrayValues.equalsObject( x, value() );
    }

    @Override
    public int hashCode()
    {
        Object myValue = value(); // value() accesses LazyValue.value, implying a read barrier ...
        return type.hashCode( myValue ); // ... so accessing type is safe
    }

    @Override
    protected Object castAndPrepareForReturn( Object value )
    {
        // this method is invoked after accessing LazyProperty.value, implying a read barrier ...
        return type.clone( value ); // ... so accessing type is safe
    }

    private enum Type
    {
        INT
        {
            @Override
            int hashCode( Object array )
            {
                return IntegralArrayValue.hash( (int[]) array );
            }

            @Override
            boolean equals( Object value, Value other )
            {
                return other.equals( (int[]) value );
            }

            @Override
            Object clone( Object array )
            {
                return ((int[]) array).clone();
            }
        },
        LONG
        {
            @Override
            int hashCode( Object array )
            {
                return IntegralArrayValue.hash( (long[]) array );
            }

            @Override
            boolean equals( Object value, Value other )
            {
                return other.equals( (long[]) value );
            }

            @Override
            Object clone( Object array )
            {
                return ((long[]) array).clone();
            }
        },
        BOOLEAN
        {
            @Override
            int hashCode( Object array )
            {
                return BooleanArrayValue.hash( (boolean[]) array );
            }

            @Override
            boolean equals( Object value, Value other )
            {
                return BooleanArrayValue.equals( (boolean[]) value, other );
            }

            @Override
            Object clone( Object array )
            {
                return ((boolean[]) array).clone();
            }
        },
        BYTE
        {
            @Override
            int hashCode( Object array )
            {
                return IntegralArrayValue.hash( (byte[]) array );
            }

            @Override
            boolean equals( Object value, Value other )
            {
                return other.equals( (byte[]) value );
            }

            @Override
            Object clone( Object array )
            {
                return ((byte[]) array).clone();
            }
        },
        DOUBLE
        {
            @Override
            int hashCode( Object array )
            {
                return FloatingPointArrayValue.hash( (double[]) array );
            }

            @Override
            boolean equals( Object value, Value other )
            {
                return other.equals( (double[]) value );
            }

            @Override
            Object clone( Object array )
            {
                return ((double[]) array).clone();
            }
        },
        STRING
        {
            @Override
            int hashCode( Object array )
            {
                return StringArrayValue.hash( (String[]) array );
            }

            @Override
            boolean equals( Object value, Value other )
            {
                return other.equals( (String[]) value );
            }

            @Override
            Object clone( Object array )
            {
                return ((String[]) array).clone();
            }
        },
        SHORT
        {
            @Override
            int hashCode( Object array )
            {
                return IntegralArrayValue.hash( (short[]) array );
            }

            @Override
            boolean equals( Object value, Value other )
            {
                return other.equals( (short[]) value );
            }

            @Override
            Object clone( Object array )
            {
                return ((short[]) array).clone();
            }
        },
        CHAR
        {
            @Override
            int hashCode( Object array )
            {
                return CharArrayValue.hash( (char[]) array );
            }

            @Override
            boolean equals( Object value, Value other )
            {
                return other.equals( (char[]) value );
            }

            @Override
            Object clone( Object array )
            {
                return ((char[]) array).clone();
            }
        },
        FLOAT
        {
            @Override
            int hashCode( Object array )
            {
                return FloatingPointArrayValue.hash( (float[]) array );
            }

            @Override
            boolean equals( Object value, Value other )
            {
                return other.equals( (float[]) value );
            }

            @Override
            Object clone( Object array )
            {
                return ((float[]) array).clone();
            }
        };

        abstract int hashCode( Object array );

        abstract boolean equals( Object value, Value other );

        abstract Object clone( Object array );

        public static Type from( Object array )
        {
            if ( !array.getClass().isArray() )
            {
                throw new IllegalArgumentException( array + " is not an array, it's a " + array.getClass() );
            }

            if ( array instanceof int[] )
            {
                return INT;
            }
            if ( array instanceof long[] )
            {
                return LONG;
            }
            if ( array instanceof boolean[] )
            {
                return BOOLEAN;
            }
            if ( array instanceof byte[] )
            {
                return BYTE;
            }
            if ( array instanceof double[] )
            {
                return DOUBLE;
            }
            if ( array instanceof String[] )
            {
                return STRING;
            }
            if ( array instanceof short[] )
            {
                return SHORT;
            }
            if ( array instanceof char[] )
            {
                return CHAR;
            }
            if ( array instanceof float[] )
            {
                return FLOAT;
            }
            throw new IllegalArgumentException( "Unrecognized array type " + array.getClass().getComponentType() );
        }
    }
}
