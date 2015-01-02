/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.api.properties;

import java.util.Arrays;
import java.util.concurrent.Callable;

import org.neo4j.kernel.impl.cache.SizeOfs;

class LazyArrayProperty extends LazyProperty<Object>
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

    LazyArrayProperty( int propertyKeyId, final Callable<?> producer )
    {
        super( propertyKeyId, producer );
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
    public boolean valueEquals( Object value )
    {
        Object myValue = value(); // value() accesses LazyProperty.value, implying a read barrier ...
        return type.equals( myValue, value ); // ... so accessing type is safe
    }

    @Override
    int valueHash()
    {
        Object myValue = value(); // value() accesses LazyProperty.value, implying a read barrier ...
        return type.hashCode( myValue ); // ... so accessing type is safe
    }

    @Override
    protected Object castAndPrepareForReturn( Object value )
    {
        // this method is invoked after accessing LazyProperty.value, implying a read barrier ...
        return type.clone( value ); // ... so accessing type is safe
    }

    @Override
    public int sizeOfObjectInBytesIncludingOverhead()
    {
        return super.sizeOfObjectInBytesIncludingOverhead() + SizeOfs.REFERENCE_SIZE;
    }

    private enum Type
    {
        INT
        {
            @Override
            int hashCode( Object array )
            {
                return Arrays.hashCode( (int[]) array );
            }

            @Override
            boolean equals( Object array1, Object array2 )
            {
                return array2 instanceof int[] && Arrays.equals( (int[]) array1, (int[]) array2 );
            }

            @Override
            Object clone( Object array )
            {
                return ((int[])array).clone();
            }
        },
        LONG
        {
            @Override
            int hashCode( Object array )
            {
                return Arrays.hashCode( (long[]) array );
            }

            @Override
            boolean equals( Object array1, Object array2 )
            {
                return array2 instanceof long[] && Arrays.equals( (long[]) array1, (long[]) array2 );
            }

            @Override
            Object clone( Object array )
            {
                return ((long[])array).clone();
            }
        },
        BOOLEAN
        {
            @Override
            int hashCode( Object array )
            {
                return Arrays.hashCode( (boolean[]) array );
            }

            @Override
            boolean equals( Object array1, Object array2 )
            {
                return array2 instanceof boolean[] && Arrays.equals( (boolean[]) array1, (boolean[]) array2 );
            }

            @Override
            Object clone( Object array )
            {
                return ((boolean[])array).clone();
            }
        },
        BYTE
        {
            @Override
            int hashCode( Object array )
            {
                return Arrays.hashCode( (byte[]) array );
            }

            @Override
            boolean equals( Object array1, Object array2 )
            {
                return array2 instanceof byte[] && Arrays.equals( (byte[]) array1, (byte[]) array2 );
            }

            @Override
            Object clone( Object array )
            {
                return ((byte[])array).clone();
            }
        },
        DOUBLE
        {
            @Override
            int hashCode( Object array )
            {
                return Arrays.hashCode( (double[]) array );
            }

            @Override
            boolean equals( Object array1, Object array2 )
            {
                return array2 instanceof double[] && Arrays.equals( (double[]) array1, (double[]) array2 );
            }

            @Override
            Object clone( Object array )
            {
                return ((double[])array).clone();
            }
        },
        STRING
        {
            @Override
            int hashCode( Object array )
            {
                return Arrays.hashCode( (String[]) array );
            }

            @Override
            boolean equals( Object array1, Object array2 )
            {
                return array2 instanceof String[] && Arrays.equals( (String[]) array1, (String[]) array2 );
            }

            @Override
            Object clone( Object array )
            {
                return ((String[])array).clone();
            }
        },
        SHORT
        {
            @Override
            int hashCode( Object array )
            {
                return Arrays.hashCode( (short[]) array );
            }

            @Override
            boolean equals( Object array1, Object array2 )
            {
                return array2 instanceof short[] && Arrays.equals( (short[]) array1, (short[]) array2 );
            }

            @Override
            Object clone( Object array )
            {
                return ((short[])array).clone();
            }
        },
        CHAR
        {
            @Override
            int hashCode( Object array )
            {
                return Arrays.hashCode( (char[]) array );
            }

            @Override
            boolean equals( Object array1, Object array2 )
            {
                return array2 instanceof char[] && Arrays.equals( (char[]) array1, (char[]) array2 );
            }

            @Override
            Object clone( Object array )
            {
                return ((char[])array).clone();
            }
        },
        FLOAT
        {
            @Override
            int hashCode( Object array )
            {
                return Arrays.hashCode( (float[]) array );
            }

            @Override
            boolean equals( Object array1, Object array2 )
            {
                return array2 instanceof float[] && Arrays.equals( (float[]) array1, (float[]) array2 );
            }

            @Override
            Object clone( Object array )
            {
                return ((float[])array).clone();
            }
        };

        abstract int hashCode( Object array );

        abstract boolean equals( Object array1, Object array2 );

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
