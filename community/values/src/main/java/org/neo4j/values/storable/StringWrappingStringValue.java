/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Implementation of StringValue that wraps a `java.lang.String` and
 * delegates methods to that instance.
 */
final class StringWrappingStringValue extends StringValue
{
    private final String value;

    StringWrappingStringValue( String value )
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
        return value.codePointCount( 0, value.length() );
    }

    @Override
    public int computeHash()
    {
        //NOTE that we are basing the hash code on code points instead of char[] values.
        if ( value.isEmpty() )
        {
            return 0;
        }
        int h = 1, length = value.length();
        for ( int offset = 0, codePoint; offset < length; offset += Character.charCount( codePoint ) )
        {
            codePoint = value.codePointAt( offset );
            h = 31 * h + codePoint;
        }
        return h;
    }

    @Override
    public TextValue substring( int start, int length )
    {
        int s = Math.min( start, length() );
        int e = Math.min( s + length, length() );
        int codePointStart = value.offsetByCodePoints( 0, s );
        int codePointEnd = value.offsetByCodePoints( 0, e );

        return Values.stringValue( value.substring( codePointStart, codePointEnd ) );
    }

    @Override
    public TextValue trim()
    {
        int start = ltrimIndex( value );
        int end = rtrimIndex( value );
        return Values.stringValue( value.substring( start, Math.max( end, start ) ) );
    }

    @Override
    public TextValue ltrim()
    {
        int start = ltrimIndex( value );
        return Values.stringValue( value.substring( start, value.length() ) );
    }

    @Override
    public TextValue rtrim()
    {
        int end = rtrimIndex( value );
        return Values.stringValue( value.substring( 0, end ) );
    }

    @Override
    public TextValue reverse()
    {
        StringBuilder stringBuilder = new StringBuilder( value() );
        return Values.stringValue( stringBuilder.reverse().toString() );
    }

    @Override
    Matcher matcher( Pattern pattern )
    {
        return pattern.matcher( value );
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
}
