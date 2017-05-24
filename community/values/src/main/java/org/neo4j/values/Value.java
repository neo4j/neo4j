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

public interface Value extends ValueGroup.WithId
{
    boolean equals( Value other );

    boolean equals( byte[] x );
    boolean equals( short[] x );
    boolean equals( int[] x );
    boolean equals( long[] x );

    boolean equals( float[] x );
    boolean equals( double[] x );

    boolean equals( boolean x );
    boolean equals( boolean[] x );

    boolean equals( char x );
    boolean equals( String x );

    boolean equals( char[] x );
    boolean equals( String[] x );

    void writeTo( ValueWriter writer );

    Object asPublic();
}
