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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.graphdb.spatial.Geometry;
import org.neo4j.hashing.HashFunction;
import org.neo4j.values.AnyValue;
import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.Comparison;
import org.neo4j.values.Equality;
import org.neo4j.values.SequenceValue;

import static java.lang.String.format;
import static org.neo4j.values.storable.Values.NO_VALUE;

public abstract class Value extends AnyValue
{
    private static final Pattern MAP_PATTERN = Pattern.compile( "\\{(.*)}" );

    private static final Pattern KEY_VALUE_PATTERN =
            Pattern.compile( "(?:\\A|,)\\s*+(?<k>[a-z_A-Z]\\w*+)\\s*:\\s*(?<v>[^\\s,]+)" );

    static final Pattern QUOTES_PATTERN = Pattern.compile( "^[\"']|[\"']$" );

    @Override
    public boolean equalTo( Object other )
    {
        return other instanceof Value && equals( (Value) other );
    }

    public abstract boolean equals( Value other );

    public boolean equals( byte[] x )
    {
        return false;
    }

    public boolean equals( short[] x )
    {
        return false;
    }

    public boolean equals( int[] x )
    {
        return false;
    }

    public boolean equals( long[] x )
    {
        return false;
    }

    public boolean equals( float[] x )
    {
        return false;
    }

    public boolean equals( double[] x )
    {
        return false;
    }

    public boolean equals( boolean x )
    {
        return false;
    }

    public boolean equals( boolean[] x )
    {
        return false;
    }

    public boolean equals( long x )
    {
        return false;
    }

    public boolean equals( double x )
    {
        return false;
    }

    public boolean equals( char x )
    {
        return false;
    }

    public boolean equals( String x )
    {
        return false;
    }

    public boolean equals( char[] x )
    {
        return false;
    }

    public boolean equals( String[] x )
    {
        return false;
    }

    public boolean equals( Geometry[] x )
    {
        return false;
    }

    public boolean equals( ZonedDateTime[] x )
    {
        return false;
    }

    public boolean equals( LocalDate[] x )
    {
        return false;
    }

    public boolean equals( DurationValue[] x )
    {
        return false;
    }

    public boolean equals( LocalDateTime[] x )
    {
        return false;
    }

    public boolean equals( LocalTime[] x )
    {
        return false;
    }

    public boolean equals( OffsetTime[] x )
    {
        return false;
    }

    @Override
    public Equality ternaryEquals( AnyValue other )
    {
        assert other != null : "null values are not supported, use NoValue.NO_VALUE instead";
        if ( other == NO_VALUE )
        {
            return Equality.UNDEFINED;
        }
        if ( other.isSequenceValue() && this.isSequenceValue() )
        {
            return ((SequenceValue) this).ternaryEquality( (SequenceValue) other );
        }
        if ( other instanceof Value && ((Value) other).valueGroup() == valueGroup() )
        {
            Value otherValue = (Value) other;
            if ( this.ternaryUndefined() || otherValue.ternaryUndefined() )
            {
                return Equality.UNDEFINED;
            }
            return equals( otherValue ) ? Equality.TRUE : Equality.FALSE;
        }
        return Equality.FALSE;
    }

    abstract int unsafeCompareTo( Value other );

    /**
     * Should return {@code Comparison.UNDEFINED} for values that cannot be compared
     * under Comparability semantics.
     */
    Comparison unsafeTernaryCompareTo( Value other )
    {
        if ( ternaryUndefined() || other.ternaryUndefined() )
        {
            return Comparison.UNDEFINED;
        }
        return Comparison.from( unsafeCompareTo( other ) );
    }

    boolean ternaryUndefined()
    {
        return false;
    }

    @Override
    public <E extends Exception> void writeTo( AnyValueWriter<E> writer ) throws E
    {
        writeTo( (ValueWriter<E>)writer );
    }

    public abstract <E extends Exception> void writeTo( ValueWriter<E> writer ) throws E;

    /**
     * Return this value as a regular java boxed primitive, String or primitive array. This method performs defensive
     * copying when needed, so the returned value is safe to modify.
     *
     * @return the object version of the current value
     */
    public abstract Object asObjectCopy();

    /**
     * Return this value as a regular java boxed primitive, String or primitive array. This method does not clone
     * primitive arrays.
     *
     * @return the object version of the current value
     */
    public Object asObject()
    {
        return asObjectCopy();
    }

    /**
     * Returns a json-like string representation of the current value.
     */
    public abstract String prettyPrint();

    public abstract ValueGroup valueGroup();

    public abstract NumberType numberType();

    /**
     * Returns whether or not the type of this value is the same as the type of the given value. Value type is more specific than
     * what {@link #valueGroup()} returns, but less granular than, say specific class. For example there are specific classes for
     * representing string values for various scenarios, but they're all strings... same type.
     *
     * @param value {@link Value} to compare type against.
     * @return {@code true} if the given {@code value} is of the same value type as this value.
     */
    public boolean isSameValueTypeAs( Value value )
    {
        return getClass() == value.getClass();
    }

    public final long hashCode64()
    {
        HashFunction xxh64 = HashFunction.incrementalXXH64();
        long seed = 1; // Arbitrary seed, but it must always be the same or hash values will change.
        return xxh64.finalise( updateHash( xxh64, xxh64.initialise( seed ) ) );
    }

    public abstract long updateHash( HashFunction hashFunction, long hash );

    static void parseHeaderInformation( CharSequence text, String type, CSVHeaderInformation info )
    {
        Matcher mapMatcher = MAP_PATTERN.matcher( text );
        String errorMessage = format( "Failed to parse %s value: '%s'", type, text );
        if ( !(mapMatcher.find() && mapMatcher.groupCount() == 1) )
        {
            throw new InvalidArgumentException( errorMessage );
        }

        String mapContents = mapMatcher.group( 1 );
        if ( mapContents.isEmpty() )
        {
            throw new InvalidArgumentException( errorMessage );
        }

        Matcher matcher = KEY_VALUE_PATTERN.matcher( mapContents );
        if ( !(matcher.find()) )
        {
            throw new InvalidArgumentException( errorMessage );
        }

        do
        {
            String key = matcher.group( "k" );
            if ( key != null )
            {
                String value = matcher.group( "v" );
                if ( value != null )
                {
                    info.assign( key, value );
                }
            }
        }
        while ( matcher.find() );
    }
}
