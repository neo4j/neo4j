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

abstract class StringValue extends TextValue
{
    abstract String value();

    @Override
    public boolean equals( Object other )
    {
        return other != null && other instanceof Value && equals( (Value) other );
    }

    @Override
    public boolean equals( Value value )
    {
        return value.equals( value() );
    }

    @Override
    public boolean equals( char x )
    {
        return value().length() == 1 && value().charAt( 0 ) == x;
    }

    @Override
    public boolean equals( String x )
    {
        return value().equals( x );
    }

    @Override
    public int hashCode()
    {
        return value().hashCode();
    }

    @Override
    public void writeTo( ValueWriter writer )
    {
        writer.writeString( value() );
    }

    @Override
    public Object asPublic()
    {
        return value();
    }

    @Override
    public String toString()
    {
        return format( "String(\"%s\")", value() );
    }

    @Override
    public int compareTo( TextValue other )
    {
        return value().compareTo( other.stringValue() );
    }

    @Override
    public String stringValue()
    {
        return value();
    }

    final static class Direct extends StringValue
    {
        final String value;

        Direct( String value )
        {
            assert value != null;
            this.value = value;
        }

        @Override
        String value()
        {
            return value;
        }
    }

    final static class Lazy extends StringValue implements LazyValue<String>
    {
        private volatile Object field;

        Lazy( Values.ValueLoader<String> producer )
        {
            this.field = producer;
        }

        @Override
        String value()
        {
            return LazyValues.getOrLoad( this );
        }

        @Override
        public void registerValue( String value )
        {
            this.field = value;
        }

        @Override
        public Object getMaybeValue()
        {
            return field;
        }
    }
}
