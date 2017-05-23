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

import static java.lang.String.format;

final class DirectChar extends DirectScalar implements ValueGroup.VText
{
    final char value;

    DirectChar( char value )
    {
        this.value = value;
    }

    @Override
    public boolean equals( Object other )
    {
        return other != null && other instanceof Value && equals( (Value) other );
    }

    @Override
    public boolean equals( Value other )
    {
        return other.equals( value );
    }

    @Override
    boolean equals( boolean x )
    {
        return false;
    }

    @Override
    boolean equals( char x )
    {
        return value == x;
    }

    @Override
    boolean equals( String x )
    {
        return x.length() == 1 && x.charAt( 0 ) == value;
    }

    @Override
    public int hashCode()
    {
        return value;
    }

    @Override
    void writeTo( ValueWriter writer )
    {
        writer.writeString( value );
    }

    @Override
    public String stringValue()
    {
        return Character.toString( value );
    }

    @Override
    public int compareTo( ValueGroup.VText other )
    {
        return TextValues.compareCharToString( value, other.stringValue() );
    }

    @Override
    public String toString()
    {
        return format( "Char('%s')", value );
    }
}
