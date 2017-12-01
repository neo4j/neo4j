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

import java.util.Arrays;

import org.neo4j.graphdb.spatial.Geometry;
import org.neo4j.values.AnyValue;
import org.neo4j.values.SequenceValue;

import static java.lang.String.format;

public abstract class PointArray extends ArrayValue
{
    abstract PointValue[] value();

    @Override
    public int length()
    {
        return value().length;
    }

    public PointValue pointValue( int offset )
    {
        return value()[offset];
    }

    @Override
    public boolean equals( Geometry[] x )
    {
        return Arrays.equals( value(), x );
    }

    @Override
    public boolean equals( Value other )
    {
        if ( other instanceof PointArray )
        {
            return Arrays.equals( this.value(), ((PointArray) other).value() );
        }
        else
        {
            return false;
        }
    }

    @Override
    public boolean equals( byte[] x )
    {
        return false;
    }

    @Override
    public boolean equals( short[] x )
    {
        return false;
    }

    @Override
    public boolean equals( int[] x )
    {
        return false;
    }

    @Override
    public boolean equals( long[] x )
    {
        return false;
    }

    @Override
    public boolean equals( float[] x )
    {
        return false;
    }

    @Override
    public boolean equals( double[] x )
    {
        return false;
    }

    @Override
    public boolean equals( boolean[] x )
    {
        return false;
    }

    @Override
    public boolean equals( char[] x )
    {
        return false;
    }

    @Override
    public boolean equals( String[] x )
    {
        return false;
    }

    @Override
    public ValueGroup valueGroup()
    {
        return ValueGroup.GEOMETRY_ARRAY;
    }

    @Override
    public NumberType numberType()
    {
        return NumberType.NO_NUMBER;
    }

    @Override
    public final boolean eq( Object other )
    {
        if ( other == null )
        {
            return false;
        }

        if ( other instanceof SequenceValue )
        {
            return this.equals( (SequenceValue) other );
        }
        return other instanceof Value && equals( (Value) other );
    }

    @Override
    public int computeHash()
    {
        return Arrays.hashCode( value() );
    }

    @Override
    public <E extends Exception> void writeTo( ValueWriter<E> writer ) throws E
    {
        PrimitiveArrayWriting.writeTo( writer, value() );
    }

    @Override
    public Object asObjectCopy()
    {
        return value().clone();
    }

    @Override
    @Deprecated
    public Object asObject()
    {
        return value();
    }

    @Override
    public String prettyPrint()
    {
        return Arrays.toString( value() );
    }

    public int compareTo( PointArray other )
    {
        int i = 0;
        int x = 0;
        int length = Math.min( this.length(), other.length() );

        while ( x == 0 && i < length )
        {
            x = this.pointValue( i ).compareTo( other.pointValue( i ) );
            i++;
        }
        if ( x == 0 )
        {
            x = this.length() - other.length();
        }
        return x;
    }

    static final class Direct extends PointArray
    {
        final PointValue[] value;

        Direct( PointValue[] value )
        {
            assert value != null;
            this.value = value;
        }

        @Override
        PointValue[] value()
        {
            return value;
        }

        @Override
        public String toString()
        {
            return format( "PointArray%s", Arrays.toString( value() ) );
        }

        @Override
        public AnyValue value( int offset )
        {
            return Values.point( value[offset] );
        }
    }
}
