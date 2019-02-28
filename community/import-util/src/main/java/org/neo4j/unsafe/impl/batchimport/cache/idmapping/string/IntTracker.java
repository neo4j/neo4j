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
package org.neo4j.unsafe.impl.batchimport.cache.idmapping.string;

import org.neo4j.unsafe.impl.batchimport.cache.IntArray;
import org.neo4j.unsafe.impl.batchimport.cache.LongBitsManipulator;

/**
 * {@link Tracker} capable of keeping {@code int} range values, using {@link IntArray}.
 * Will fail in {@link #set(long, long)} with {@link ArithmeticException} if trying to put a too big value.
 */
public class IntTracker extends AbstractTracker<IntArray>
{
    static final int SIZE = Integer.BYTES;
    static final int ID_BITS = Byte.SIZE * SIZE - 1;
    static final long MAX_ID = (1 << ID_BITS) - 1;
    static final int DEFAULT_VALUE = -1;
    private static final LongBitsManipulator BITS = new LongBitsManipulator( ID_BITS, 1 );

    public IntTracker( IntArray array )
    {
        super( array );
    }

    @Override
    public long get( long index )
    {
        return BITS.get( array.get( index ), 0 );
    }

    /**
     * @throws ArithmeticException if value is bigger than {@link Integer#MAX_VALUE}.
     */
    @Override
    public void set( long index, long value )
    {
        long field = array.get( index );
        field = BITS.set( field, 0, value );
        array.set( index, (int) field );
    }

    @Override
    public void markAsDuplicate( long index )
    {
        long field = array.get( index );
        // Since the default value for the whole field is -1 (i.e. all 1s) then this mark will have to be 0.
        field = BITS.set( field, 1, 0 );
        array.set( index, (int) field );
    }

    @Override
    public boolean isMarkedAsDuplicate( long index )
    {
        long field = array.get( index );
        return BITS.get( field, 1 ) == 0;
    }
}
