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

import org.neo4j.values.virtual.ListValue;

import static java.lang.String.format;
import static org.neo4j.values.storable.Values.stringArray;
import static org.neo4j.values.virtual.VirtualValues.fromArray;

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
        String[] split = asString.split( separator );
        return fromArray( stringArray( split ) );
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
        return format( "String(\"%s\")", value() );
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
        return len1 - len2;
    }

    static TextValue EMTPY = new StringValue()
    {
        @Override
        protected int computeHash()
        {
            return 0;
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
        String value()
        {
            return "";
        }
    };
}

