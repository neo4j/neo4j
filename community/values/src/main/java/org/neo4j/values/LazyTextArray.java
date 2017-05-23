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

import java.util.concurrent.Callable;

abstract class LazyTextArray<T> extends LazyArray<T> implements ValueGroup.VTextArray
{
    LazyTextArray( Callable<? extends T> producer )
    {
        super( producer );
    }

    @Override
    boolean equals( byte[] x )
    {
        return false;
    }

    @Override
    boolean equals( short[] x )
    {
        return false;
    }

    @Override
    boolean equals( int[] x )
    {
        return false;
    }

    @Override
    boolean equals( long[] x )
    {
        return false;
    }

    @Override
    boolean equals( float[] x )
    {
        return false;
    }

    @Override
    boolean equals( double[] x )
    {
        return false;
    }

    @Override
    boolean equals( boolean[] x )
    {
        return false;
    }
}
