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

import java.util.Arrays;

import org.neo4j.unsafe.impl.batchimport.cache.ByteArray;
import org.neo4j.unsafe.impl.batchimport.cache.LongBitsManipulator;

/**
 * {@link Tracker} capable of keeping 6B range values, using {@link ByteArray}.
 */
public class BigIdTracker extends AbstractTracker<ByteArray>
{
    static final int SIZE = 5;
    static final int ID_BITS = (Byte.SIZE * SIZE) - 1;
    static final byte[] DEFAULT_VALUE;
    public static final long MAX_ID = 1L << ID_BITS - 1;
    private static final LongBitsManipulator BITS = new LongBitsManipulator( ID_BITS, 1 );
    static
    {
        DEFAULT_VALUE = new byte[SIZE];
        Arrays.fill( DEFAULT_VALUE, (byte) -1 );
    }

    public BigIdTracker( ByteArray array )
    {
        super( array );
    }

    @Override
    public long get( long index )
    {
        return BITS.get( array.get5ByteLong( index, 0 ), 0 );
    }

    @Override
    public void set( long index, long value )
    {
        long field = array.get5ByteLong( index, 0 );
        field = BITS.set( field, 0, value );
        array.set5ByteLong( index, 0, field );
    }

    @Override
    public void markAsDuplicate( long index )
    {
        long field = array.get5ByteLong( index, 0 );
        field = BITS.set( field, 1, 0 );
        array.set5ByteLong( index, 0, field );
    }

    @Override
    public boolean isMarkedAsDuplicate( long index )
    {
        long field = array.get5ByteLong( index, 0 );
        return BITS.get( field, 1 ) == 0;
    }
}
