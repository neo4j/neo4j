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
package org.neo4j.internal.values;

/**
 * Value that can exist transiently during computations, but that cannot be stored as a property value. A Virtual
 * Value could be a NodeReference for example.
 */
public abstract class VirtualValue extends Value
{
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
    public final boolean equals( boolean x )
    {
        return false;
    }

    @Override
    public final boolean equals( boolean[] x )
    {
        return false;
    }

    @Override
    public final boolean equals( char x )
    {
        return false;
    }

    @Override
    public final boolean equals( String x )
    {
        return false;
    }

    @Override
    public final boolean equals( char[] x )
    {
        return false;
    }

    @Override
    public final boolean equals( String[] x )
    {
        return false;
    }

    @Override
    public final boolean equals( Object other )
    {
        return other != null && other instanceof VirtualValue && equals( (VirtualValue) other );
    }

    @Override
    public final boolean equals( Value other )
    {
        return other != null && other instanceof VirtualValue && equals( (VirtualValue) other );
    }

    @Override
    public final int hashCode()
    {
        return hash();
    }

    public abstract int hash();

    public abstract boolean equals( VirtualValue other );

    @Override
    public final ValueGroup valueGroup()
    {
        return ValueGroup.VIRTUAL;
    }

    @Override
    public final NumberType numberType()
    {
        return NumberType.NO_NUMBER;
    }
}
