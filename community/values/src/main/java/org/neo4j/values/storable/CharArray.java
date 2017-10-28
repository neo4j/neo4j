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

public abstract class CharArray extends TextArray
{
    abstract char[] value();

    @Override
    public boolean equals( Value other )
    {
        return other.equals( value() );
    }

    // TODO: should we support this?
//    @Override
//    boolean equals( String x )
//    {
//        return false;
//    }

    @Override
    public boolean equals( char[] x )
    {
        return Arrays.equals( value(), x );
    }

    @Override
    public boolean equals( String[] x )
    {
        return PrimitiveArrayValues.equals( value(), x );
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
        return NumberValues.hash( value() );
    }

    @Override
    public int length()
    {
        return value().length;
    }

    @Override
    public String stringValue( int offset )
    {
        return Character.toString( value()[offset] );
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

    @Override
    public String prettyPrint()
    {
        return Arrays.toString( value() );
    }

    @Override
    public AnyValue value( int position )
    {
        return Values.charValue( value()[position] );
    }

    static final class Direct extends CharArray
    {
        final char[] value;

        Direct( char[] value )
        {
            assert value != null;
            this.value = value;
        }

        @Override
        char[] value()
        {
            return value;
        }

        @Override
        public String toString()
        {
            return format( "CharArray%s", Arrays.toString( value() ) );
        }
    }
}
