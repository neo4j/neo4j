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
package org.neo4j.unsafe.impl.batchimport.cache.idmapping.string;

import org.neo4j.unsafe.impl.batchimport.Utils;
import org.neo4j.unsafe.impl.batchimport.cache.IntArray;

/**
 * {@link Tracker} capable of keeping {@code int} range values, using {@link IntArray}.
 * Will fail in {@link #set(long, long)} with {@link ArithmeticException} if trying to put a too big value.
 */
public class IntTracker extends AbstractTracker<IntArray>
{
    public IntTracker( IntArray array )
    {
        super( array );
    }

    @Override
    public long get( long index )
    {
        return array.get( index );
    }

    /**
     * @throws ArithmeticException if value is bigger than {@link Integer#MAX_VALUE}.
     */
    @Override
    public void set( long index, long value )
    {
        array.set( index, Utils.safeCastLongToInt( value ) );
    }
}
