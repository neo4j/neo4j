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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.hashing.HashFunction;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.virtual.ListValue;

import static java.lang.String.format;
import static org.neo4j.values.virtual.VirtualValues.list;

public final class CharValue extends TextValue
{
    final char value;

    CharValue( char value )
    {
        this.value = value;
    }

    @Override
    public boolean eq( Object other )
    {
        return other != null && other instanceof Value && equals( (Value) other );
    }

    @Override
    public boolean equals( Value other )
    {
        return other.equals( value );
    }

    @Override
    public boolean equals( char x )
    {
        return value == x;
    }

    @Override
    public boolean equals( String x )
    {
        return x.length() == 1 && x.charAt( 0 ) == value;
    }

    @Override
    public int computeHash()
    {
        //The 31 is there to give it the same hash as the string equivalent
        return 31 + value;
    }

    @Override
    public long updateHash( HashFunction hashFunction, long hash )
    {
        return updateHash( hashFunction, hash, value );
    }

    public static long updateHash( HashFunction hashFunction, long hash, char value )
    {
        hash = hashFunction.update( hash, value );
        return hashFunction.update( hash, 1 ); // Pretend we're a string of length 1.
    }

    @Override
    public <E extends Exception> void writeTo( ValueWriter<E> writer ) throws E
    {
        writer.writeString( value );
    }

    @Override
    public Object asObjectCopy()
    {
        return value;
    }

    @Override
    public String prettyPrint()
    {
        return format( "'%s'", value );
    }

    @Override
    public String stringValue()
    {
        return Character.toString( value );
    }

    @Override
    public int length()
    {
        return 1;
    }

    @Override
    public TextValue substring( int start, int length )
    {
        if ( length != 1 && start != 0 )
        {
            return StringValue.EMTPY;
        }

        return this;
    }

    @Override
    public TextValue trim()
    {
        if ( Character.isWhitespace( value ) )
        {
            return StringValue.EMTPY;
        }
        else
        {
            return this;
        }
    }

    @Override
    public TextValue ltrim()
    {
        return trim();
    }

    @Override
    public TextValue rtrim()
    {
        return trim();
    }

    @Override
    public TextValue toLower()
    {
        return new CharValue( Character.toLowerCase( value ) );
    }

    @Override
    public TextValue toUpper()
    {
        return new CharValue( Character.toUpperCase( value ) );
    }

    @Override
    public ListValue split( String separator )
    {
        if ( separator.equals( stringValue() ) )
        {
            return EMPTY_SPLIT;
        }
        else
        {
            return list( Values.stringValue( stringValue() ) );
        }
    }

    @Override
    public TextValue replace( String find, String replace )
    {
        assert find != null;
        assert replace != null;
        if ( stringValue().equals( find ) )
        {
            return Values.stringValue( replace );
        }
        else
        {
            return this;
        }
    }

    @Override
    public TextValue reverse()
    {
        return this;
    }

    public char value()
    {
        return value;
    }

    @Override
    public int compareTo( TextValue other )
    {
        return TextValues.compareCharToString( value, other.stringValue() );
    }

    @Override
    public <T> T map( ValueMapper<T> mapper )
    {
        return mapper.mapChar( this );
    }

    @Override
    Matcher matcher( Pattern pattern )
    {
        return pattern.matcher( "" + value ); // TODO: we should be able to do this without allocation
    }

    @Override
    public String toString()
    {
        return format( "%s('%s')", getTypeName(), value );
    }

    @Override
    public String getTypeName()
    {
        return "Char";
    }
}
