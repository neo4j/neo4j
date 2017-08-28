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

import java.nio.charset.StandardCharsets;

import static java.lang.String.format;

public abstract class StringValue extends TextValue
{
    abstract String value();

    @Override
    public boolean eq( Object other )
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
    public int computeHash()
    {
        return value().hashCode();
    }

    @Override
    public <E extends Exception> void writeTo( ValueWriter<E> writer ) throws E
    {
        writer.writeString( value() );
    }

    @Override
    public Object asObjectCopy()
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

    @Override
    public String prettyPrint()
    {
        return format( "'%s'", value() );
    }

    static final class Direct extends StringValue
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

        @Override
        public int length()
        {
            return value.length();
        }
    }

    /*
     * Just as a normal StringValue but is backed by a byte array and does string
     * serialization lazily.
      *
      * TODO in this implementation most operation will actually load the string
      * such as hashCode, length, equals etc. These could be implemented using
      * the byte array directly
     */
    static final class UTF8StringValue extends StringValue
    {
        private volatile String value;
        private final byte[] bytes;
        private final int offset;
        private final int length;

        UTF8StringValue( byte[] bytes, int offset, int length )
        {
            assert bytes != null;
            this.bytes = bytes;
            this.offset = offset;
            this.length = length;
        }

        @Override
        public <E extends Exception> void writeTo( ValueWriter<E> writer ) throws E
        {
            writer.writeUTF8( bytes, offset, length );
        }

        @Override
        String value()
        {
            String s = value;
            if ( s == null )
            {
                synchronized ( this )
                {
                    s = value;
                    if ( s == null )
                    {
                        s = value = new String( bytes, offset, length, StandardCharsets.UTF_8 );

                    }
                }
            }
            return s;
        }

        @Override
        public int length()
        {
            return value().length();
        }
    }
}

