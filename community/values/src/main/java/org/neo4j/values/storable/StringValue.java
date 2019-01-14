/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
        String thisString = value();
        String thatString = other.stringValue();
        return thisString.compareTo( thatString );
    }

    static TextValue EMTPY = new StringValue()
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

