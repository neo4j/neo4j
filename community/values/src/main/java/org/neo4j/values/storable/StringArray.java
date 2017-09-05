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
package org.neo4j.values.storable;

import java.util.Arrays;

import org.neo4j.values.AnyValue;
import org.neo4j.values.SequenceValue;

import static java.lang.String.format;

public abstract class StringArray extends TextArray
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
    public final boolean eq( Object other )
    {
        if ( other == null )
        {
            return false;
        }

        if ( other instanceof SequenceValue )
        {
            return this.equals( (SequenceValue) other );
        }
        return other instanceof Value && equals( (Value) other );
    }

    @Override
    public int computeHash()
    {
        return Arrays.hashCode( value() );
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

    @Override
    public AnyValue value( int offset )
    {
        return Values.stringValue( stringValue( offset ) );
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
}
