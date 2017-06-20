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

abstract class StringArray extends TextArray
{
    abstract String[] value();

    @Override
    public int length()
    {
        return value().length;
    }

    @Override
    public String stringValue( int offset )
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
        return other.equals( value() );
    }

    @Override
    public boolean equals( char[] x )
    {
        return PrimitiveArrayValues.equals( x, value() );
    }

    @Override
    public boolean equals( String[] x )
    {
        return Arrays.equals( value(), x );
    }

    @Override
    public int hashCode()
    {
        return TextValues.hash( value() );
    }

    @Override
    public <E extends Exception> void writeTo( ValueWriter<E> writer ) throws E
    {
        PrimitiveArrayWriting.writeTo( writer, value() );
    }

    @Override
    public Object asObjectCopy()
    {
        return value().clone();
    }

    @Override
    @Deprecated
    public Object asObject()
    {
        return value();
    }

    public int compareTo( TextArray other )
    {
        return TextValues.compareTextArrays( this, other );
    }

    @Override
    public String prettyPrint()
    {
        return Arrays.toString( value() );
    }

    static final class Direct extends StringArray
    {
        final String[] value;

        Direct( String[] value )
        {
            assert value != null;
            this.value = value;
        }

        @Override
        String[] value()
        {
            return value;
        }

        @Override
        public String toString()
        {
            return format( "StringArray%s", Arrays.toString( value() ) );
        }
    }

    static final class Lazy extends StringArray implements LazyValue<String[]>
    {
        private volatile Object field;

        Lazy( ValueLoader<String[]> producer )
        {
            this.field = producer;
        }

        @Override
        String[] value()
        {
            return LazyValues.getOrLoad( this );
        }

        @Override
        public void registerValue( String[] value )
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
            return format( "StringArray%s",
                    LazyValues.valueIsLoaded( field ) ? Arrays.toString( value() ) : "?" );
        }
    }
}
