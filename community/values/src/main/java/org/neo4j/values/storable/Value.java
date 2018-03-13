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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;

import org.neo4j.graphdb.spatial.Geometry;
import org.neo4j.values.AnyValue;
import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.SequenceValue;

import static org.neo4j.values.storable.Values.NO_VALUE;

public abstract class Value extends AnyValue
{
    @Override
    public boolean eq( Object other )
    {
        return other != null && other instanceof Value && equals( (Value) other );
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
    public Boolean ternaryEquals( AnyValue other )
    {
        if ( other == null || other == NO_VALUE )
        {
            return null;
        }
        if ( other.isSequenceValue() && this.isSequenceValue() )
        {
            return ((SequenceValue) this).ternaryEquality( (SequenceValue) other );
        }
        if ( other instanceof Value && ((Value) other).valueGroup() == valueGroup() )
        {
            Value otherValue = (Value) other;
            if ( this.isNaN() || otherValue.isNaN() )
            {
                return null;
            }
            return equals( otherValue );
        }
        return Boolean.FALSE;
    }

    abstract int unsafeCompareTo( Value other );

    /**
     * Should return {@code null} for values that cannot be compared
     * under Comparability semantics.
     */
    Integer unsafeTernaryCompareTo( Value other )
    {
        return unsafeCompareTo( other );
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

    public boolean isNaN()
    {
        return false;
    }
}
