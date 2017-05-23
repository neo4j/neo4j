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

abstract class DirectFloatingPointArray extends ArrayValue implements ValueGroup.VFloatingPointArray
{
    public abstract double doubleValue( int index );

    @Override
    boolean equals( boolean[] x )
    {
        return false;
    }

    @Override
    boolean equals( char[] x )
    {
        return false;
    }

    @Override
    boolean equals( String[] x )
    {
        return false;
    }

    @Override
    public boolean equals( Object other )
    {
        return other != null && other instanceof Value && equals( (Value) other );
    }

    @Override
    final boolean equals( Value other )
    {
        if ( other instanceof ValueGroup.VFloatingPointArray )
        {
            ValueGroup.VFloatingPointArray that = (ValueGroup.VFloatingPointArray) other;
            return NumberValues.numbersEqual( this, that );
        }
        else if ( other instanceof ValueGroup.VIntegerArray )
        {
            ValueGroup.VIntegerArray that = (ValueGroup.VIntegerArray) other;
            return NumberValues.numbersEqual( this, that );
        }
        return false;
    }

    @Override
    public final int hashCode()
    {
        int result = 1;
        for ( int i = 0, len = length(); i < len; i++ )
        {
            int elementHash = NumberValues.hash( doubleValue( i ) );
            result = 31 * result + elementHash;
        }
        return result;
    }

    public int compareTo( ValueGroup.VIntegerArray other )
    {
        int i = 0;
        int length = length();
        int x = length - other.length();

        while ( x == 0 && i < length )
        {
            x = NumberValues.compareDoubleAgainstLong( doubleValue( i ), other.longValue( i ) );
            i++;
        }
        return x;
    }

    public int compareTo( ValueGroup.VFloatingPointArray other )
    {
        int i = 0;
        int length = length();
        int x = length - other.length();

        while ( x == 0 && i < length )
        {
            x = Double.compare( doubleValue( i ), other.doubleValue( i ) );
            i++;
        }
        return x;
    }
}
