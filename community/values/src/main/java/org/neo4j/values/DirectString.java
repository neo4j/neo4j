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

final class DirectString extends DirectScalar implements ValueGroup.VText
{
    final String string;

    DirectString( String string )
    {
        assert string != null;
        this.string = string;
    }

    @Override
    public boolean equals( Object other )
    {
        return other != null && other instanceof Value && equals( (Value) other );
    }

    @Override
    public boolean equals( Value value )
    {
        return value.equals( string );
    }

    @Override
    public boolean equals( boolean x )
    {
        return false;
    }

    @Override
    public boolean equals( char x )
    {
        return string.length() == 1 && string.charAt( 0 ) == x;
    }

    @Override
    public boolean equals( String x )
    {
        return string.equals( x );
    }

    @Override
    public int hashCode()
    {
        return string.hashCode();
    }

    @Override
    public void writeTo( ValueWriter writer )
    {
        writer.writeString( string );
    }

    @Override
    public Object asPublic()
    {
        return string;
    }

    @Override
    public String toString()
    {
        return format( "String(\"%s\")", string );
    }

    @Override
    public int compareTo( ValueGroup.VText other )
    {
        return string.compareTo( other.stringValue() );
    }

    @Override
    public String stringValue()
    {
        return string;
    }
}
