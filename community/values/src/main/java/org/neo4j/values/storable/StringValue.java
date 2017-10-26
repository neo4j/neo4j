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
        //NOTE that we are basing the hash code on code points instead of char[] values.
        String value = value();
        if ( value.isEmpty() )
        {
            return 0;
        }
        int h = 1;
        final int length = value.length();
        for ( int offset = 0; offset < length; )
        {
            final int codePoint = value.codePointAt( offset );
            h = 31 * h + codePoint;
            offset += Character.charCount( codePoint );
        }
        return h;
    }

    @Override
    public TextValue substring( int start, int end )
    {
        assert start >= 0;
        assert end < length();
        int subLen = end - start;
        assert subLen >= 0;
        String value = value();

        int codePointStart = value.offsetByCodePoints( 0, start );
        int codePointEnd = value.offsetByCodePoints( 0, end );

        return Values.stringValue( value.substring( codePointStart, codePointEnd ) );
    }

    @Override
    public TextValue trim()
    {
        String value = value();
        int start = ltrimIndex( value );
        int end = rtrimIndex( value );
        return Values.stringValue( value.substring( start, Math.max( end, start ) ) );
    }

    @Override
    public TextValue ltrim()
    {
        String value = value();
        int start = ltrimIndex( value );
        return Values.stringValue( value.substring( start, value.length() ) );
    }

    @Override
    public TextValue rtrim()
    {
        String value = value();
        int end = rtrimIndex( value );
        return Values.stringValue( value.substring( 0, end ) );
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
        String thisString = value();
        String thatString = other.stringValue();
        int len1 = thisString.length();
        int len2 = thatString.length();
        int lim = Math.min( len1, len2 );

        int k = 0;
        while ( k < lim )
        {
            int c1 = thisString.codePointAt( k );
            int c2 = thatString.codePointAt( k );
            if ( c1 != c2 )
            {
                return c1 - c2;
            }
            k += Character.charCount( c1 );
        }
        return length() - other.length();
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

    private int ltrimIndex( String value )
    {
        int start = 0, length = value.length();
        while ( start < length )
        {
            int codePoint = value.codePointAt( start );
            if ( !Character.isWhitespace( codePoint ) )
            {
                break;
            }
            start += Character.charCount( codePoint );
        }

        return start;
    }

    private int rtrimIndex( String value )
    {
        int end = value.length();
        while ( end > 0 )
        {
            int codePoint = value.codePointBefore( end );
            if ( !Character.isWhitespace( codePoint ) )
            {
                break;
            }
            end--;
        }
        return end;
    }

    static TextValue EMTPY = new StringValue()
    {
        @Override
        public int length()
        {
            return 0;
        }

        @Override
        public TextValue trim()
        {
            return this;
        }

        @Override
        public TextValue ltrim()
        {
            return this;
        }

        @Override
        public TextValue rtrim()
        {
            return this;
        }

        @Override
        String value()
        {
            return "";
        }
    };
}

