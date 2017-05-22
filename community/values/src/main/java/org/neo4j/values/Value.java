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
package org.neo4j.values;

public abstract class Value
{
    enum Type
    {
        INTEGER,
        FLOAT,
        BOOLEAN,
        STRING,
        ARRAY,
        MAP
    }

    @Override
    public boolean equals( Object other )
    {
        throw new UnsupportedOperationException( "You forgot to implement `equals()` in concrete Value class!" );
    }

    @Override
    public int hashCode()
    {
        throw new UnsupportedOperationException( "You forgot to implement `hashCode()` in concrete Value class!" );
    }

    abstract boolean equals( Value other );

    abstract boolean equals( byte[] x );
    abstract boolean equals( short[] x );
    abstract boolean equals( int[] x );
    abstract boolean equals( long[] x );

    abstract boolean equals( float[] x );
    abstract boolean equals( double[] x );

    abstract boolean equals( boolean x );
    abstract boolean equals( boolean[] x );

    abstract boolean equals( char x );
    abstract boolean equals( String x );

    abstract boolean equals( char[] x );
    abstract boolean equals( String[] x );

    public interface WithStringValue
    {
        String stringValue();
    }
}
