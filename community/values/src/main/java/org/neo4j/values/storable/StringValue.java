/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.hashing.HashFunction;
import org.neo4j.values.AnyValue;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.storable.helpers.UnsafeStringUtils;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.VirtualValues;

import static java.lang.String.format;

public abstract class StringValue extends TextValue
{
    abstract String value();

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
    public <E extends Exception> void writeTo( ValueWriter<E> writer ) throws E
    {
        writer.writeString( value() );
    }

    @Override
    public TextValue toLower()
    {
        return new StringWrappingStringValue( value().toLowerCase() );
    }

    @Override
    public TextValue toUpper()
    {
        return new StringWrappingStringValue( value().toUpperCase() );
    }

    @Override
    public ListValue split( String separator )
    {
        assert separator != null;
        String asString = value();
        //Cypher has different semantics for the case where the separator
        //is exactly the value, in cypher we expect two empty arrays
        //where as java returns an empty array
        if ( separator.equals( asString ) )
        {
            return EMPTY_SPLIT;
        }
        else if ( separator.isEmpty() )
        {
            return VirtualValues.fromArray( Values.charArray( asString.toCharArray() ) );
        }

        List<AnyValue> split = splitNonRegex( asString, separator );
        return VirtualValues.fromList( split );
    }

    /**
     * Splits a string.
     *
     * @param input String to be split
     * @param delim delimiter, must not be not empty
     * @return the split string as a List of TextValues
     */
    private static List<AnyValue> splitNonRegex( String input, String delim )
    {
        List<AnyValue> l = new ArrayList<>();
        int offset = 0;

        while ( true )
        {
            int index = input.indexOf( delim, offset );
            if ( index == -1 )
            {
                String substring = input.substring( offset );
                l.add( Values.stringValue( substring ) );
                return l;
            }
            else
            {
                String substring = input.substring( offset, index );
                l.add( Values.stringValue( substring ) );
                offset = index + delim.length();
            }
        }
    }

    @Override
    public TextValue replace( String find, String replace )
    {
        assert find != null;
        assert replace != null;

        return Values.stringValue( value().replace( find, replace ) );
    }

    @Override
    public Object asObjectCopy()
    {
        return value();
    }

    @Override
    public String toString()
    {
        return format( "%s(\"%s\")", getTypeName(), value() );
    }

    @Override
    public String getTypeName()
    {
        return "String";
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

    @Override
    public <T> T map( ValueMapper<T> mapper )
    {
        return mapper.mapString( this );
    }

    @Override
    public int compareTo( TextValue other )
    {
        /*
         * The normal String::compareTo contrary to what documentation claims sort in code point order. It does not
         * properly handle chars in the surrogate range. This leads to inconsistent sort-orders when mixing
         * with UT8StringValue.
         *
         * This is based on https://ssl.icu-project.org/docs/papers/utf16_code_point_order.html. The basic idea
         * is to first check for identical string prefixes and only when we find two different chars we perform
         * a fix-up if necessary before comparing.
         */

        if ( this == other )
        {
            return 0;
        }

        final String thisString = value();
        final String thatString = other.stringValue();
        final char[] chars1 = UnsafeStringUtils.toCharArray( thisString );
        final char[] chars2 = UnsafeStringUtils.toCharArray( thatString );
        final int offset1 = UnsafeStringUtils.offsetOf( thisString );
        final int offset2 = UnsafeStringUtils.offsetOf( thatString );
        final int l1 = chars1.length;
        final int l2 = chars2.length;
        char c1, c2;
        int pos = 0;

        //First compare identical substrings, here we need no fix-up
        while ( true )
        {
            //if we are at the end any of the strings they are the same
            if ( pos >= l1 || pos >= l2 )
            {
                return l1 - l2;
            }
            c1 = chars1[ pos + offset1 ];
            c2 = chars2[ pos + offset2 ];
            if ( c1 == c2 )
            {
                pos++;
            }
            else
            {
                break;
            }
        }

        //We found c1, and c2 where c1 != c2, before comparing we need
        //to perform fix-up if they are in surrogate range before comparing.
        return normalizeChars( c1, c2 );
    }

    private int normalizeChars( char c1, char c2 )
    {
        if ( c1 >= Character.MIN_HIGH_SURROGATE && c2 >= Character.MIN_HIGH_SURROGATE )
        {
            if ( c1 >= '\ue000' )
            {
                c1 -= '\u0800';
            }
            else
            {
                c1 += '\u2000';
            }
            if ( c2 >= '\ue000' )
            {
                c2 -= '\u0800';
            }
            else
            {
                c2 += '\u2000';
            }
        }

        return (int) c1 - (int) c2;
    }

    static TextValue EMPTY = new StringValue()
    {
        @Override
        protected int computeHash()
        {
            return 0;
        }

        @Override
        public long updateHash( HashFunction hashFunction, long hash )
        {
            return hashFunction.update( hash, 0 ); // Mix in our length; a single zero.
        }

        @Override
        public int length()
        {
            return 0;
        }

        @Override
        public TextValue substring( int start, int end )
        {
            return this;
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
        public TextValue reverse()
        {
            return this;
        }

        @Override
        public TextValue plus( TextValue other )
        {
            return other;
        }

        @Override
        public TextValue toLower()
        {
            return this;
        }

        @Override
        public TextValue toUpper()
        {
            return this;
        }

        @Override
        public TextValue replace( String find, String replace )
        {
            if ( find.isEmpty() )
            {
                return Values.stringValue( replace );
            }
            else
            {
                return this;
            }
        }

        @Override
        public int compareTo( TextValue other )
        {
            return -other.length();
        }

        @Override
        Matcher matcher( Pattern pattern )
        {
            return pattern.matcher( "" );
        }

        @Override
        String value()
        {
            return "";
        }
    };
}

