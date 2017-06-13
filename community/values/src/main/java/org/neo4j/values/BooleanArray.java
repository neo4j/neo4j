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

import java.util.Arrays;

import static java.lang.String.format;

abstract class BooleanArray extends ArrayValue
{
    abstract boolean[] value();

    @Override
    public int length()
    {
        return value().length;
    }

    public boolean booleanValue( int offset )
    {
        return value()[offset];
    }

    @Override
    public boolean equals( Object other )
    {
        return other != null && other instanceof Value && equals( (Value) other );
    }

    @Override
    public boolean equals( Value other )
    {
        return other.equals( this.value() );
    }

    @Override
    public boolean equals( byte[] x )
    {
        return false;
    }

    @Override
    public boolean equals( short[] x )
    {
        return false;
    }

    @Override
    public boolean equals( int[] x )
    {
        return false;
    }

    @Override
    public boolean equals( long[] x )
    {
        return false;
    }

    @Override
    public boolean equals( float[] x )
    {
        return false;
    }

    @Override
    public boolean equals( double[] x )
    {
        return false;
    }

    @Override
    public boolean equals( boolean[] x )
    {
        return Arrays.equals( value(), x );
    }

    @Override
    public boolean equals( char[] x )
    {
        return false;
    }

    @Override
    public boolean equals( String[] x )
    {
        return false;
    }

    @Override
    public int hashCode()
    {
        return NumberValues.hash( value() );
    }

    @Override
    public void writeTo( ValueWriter writer )
    {
        PrimitiveArrayWriting.writeTo( writer, value() );
    }

    @Override
    public Object asPublic()
    {
        return value().clone();
    }

    @Override
    @Deprecated
    public Object asLegacyObject()
    {
        return value();
    }

    public int compareTo( BooleanArray other )
    {
        return NumberValues.compareBooleanArrays( this, other );
    }

    public ValueGroup valueGroup()
    {
        return ValueGroup.BOOLEAN_ARRAY;
    }

    @Override
    public NumberType numberType()
    {
        return NumberType.NO_NUMBER;
    }

    @Override
    public String prettyPrint()
    {
        return Arrays.toString( value() );
    }

    static final class Direct extends BooleanArray
    {
        private final boolean[] value;

        Direct( boolean[] value )
        {
            assert value != null;
            this.value = value;
        }

        @Override
        boolean[] value()
        {
            return value;
        }

        @Override
        public String toString()
        {
            return format( "BooleanArray%s", Arrays.toString( value() ) );
        }
    }

    static final class Lazy extends BooleanArray implements LazyValue<boolean[]>
    {
        private volatile Object field;

        Lazy( Values.ValueLoader<boolean[]> producer )
        {
            this.field = producer;
        }

        @Override
        boolean[] value()
        {
            return LazyValues.getOrLoad( this );
        }

        @Override
        public void registerValue( boolean[] value )
        {
            this.field = value;
        }

        @Override
        public Object getMaybeValue()
        {
            return field;
        }

        @Override
        public String toString()
        {
            return format( "BooleanArray%s",
                    LazyValues.valueIsLoaded( field ) ? Arrays.toString( value() ) : "?" );
        }
    }
}
