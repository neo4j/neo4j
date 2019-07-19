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

public abstract class TextArray extends ArrayValue
{
    public abstract String stringValue( int offset );

    @Override
    int unsafeCompareTo( Value otherValue )
    {
        return TextValues.compareTextArrays( this, (TextArray) otherValue );
    }

    @Override
    public final boolean equals( byte[] x )
    {
        return false;
    }

    @Override
    public final boolean equals( short[] x )
    {
        return false;
    }

    @Override
    public final boolean equals( int[] x )
    {
        return false;
    }

    @Override
    public final boolean equals( long[] x )
    {
        return false;
    }

    @Override
    public final boolean equals( float[] x )
    {
        return false;
    }

    @Override
    public final boolean equals( double[] x )
    {
        return false;
    }

    @Override
    public final boolean equals( boolean[] x )
    {
        return false;
    }

    @Override
    public ValueGroup valueGroup()
    {
        return ValueGroup.TEXT_ARRAY;
    }

    @Override
    public NumberType numberType()
    {
        return NumberType.NO_NUMBER;
    }
}
