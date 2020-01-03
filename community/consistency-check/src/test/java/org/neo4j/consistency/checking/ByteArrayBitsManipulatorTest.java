/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.consistency.checking;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.unsafe.impl.batchimport.cache.ByteArray;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.consistency.checking.ByteArrayBitsManipulator.MAX_BYTES;
import static org.neo4j.consistency.checking.ByteArrayBitsManipulator.MAX_SLOT_BITS;
import static org.neo4j.consistency.checking.ByteArrayBitsManipulator.MAX_SLOT_VALUE;

@ExtendWith( RandomExtension.class )
class ByteArrayBitsManipulatorTest
{
    @Inject
    protected RandomRule random;

    @Test
    void shouldHandleMaxSlotSize()
    {
        // given
        ByteArrayBitsManipulator manipulator = new ByteArrayBitsManipulator( MAX_SLOT_BITS, 1 );
        long[][] actual = new long[1_000][];
        try ( ByteArray array = NumberArrayFactory.HEAP.newByteArray( actual.length, new byte[MAX_BYTES] ) )
        {
            // when
            for ( int i = 0; i < actual.length; i++ )
            {
                actual[i] = new long[] {random.nextLong( MAX_SLOT_VALUE + 1 ), random.nextBoolean() ? -1 : 0};
                put( manipulator, array, i, actual[i] );
            }

            for ( int i = 0; i < actual.length; i++ )
            {
                verify( manipulator, array, i, actual[i] );
            }
        }
    }

    @Test
    void shouldHandleTwoMaxSlotsAndSomeBooleans()
    {
        // given
        ByteArrayBitsManipulator manipulator = new ByteArrayBitsManipulator( MAX_SLOT_BITS, MAX_SLOT_BITS, 1, 1, 1, 1 );
        long[][] actual = new long[1_000][];
        try ( ByteArray array = NumberArrayFactory.HEAP.newByteArray( actual.length, new byte[MAX_BYTES] ) )
        {
            // when
            for ( int i = 0; i < actual.length; i++ )
            {
                actual[i] = new long[] {
                        random.nextLong( MAX_SLOT_VALUE + 1 ), random.nextLong( MAX_SLOT_VALUE + 1 ),
                        random.nextBoolean() ? -1 : 0, random.nextBoolean() ? -1 : 0, random.nextBoolean() ? -1 : 0, random.nextBoolean() ? -1 : 0};
                put( manipulator, array, i, actual[i] );
            }

            // then
            for ( int i = 0; i < actual.length; i++ )
            {
                verify( manipulator, array, i, actual[i] );
            }
        }
    }

    @Test
    void shouldHandleMinusOne()
    {
        // given
        ByteArrayBitsManipulator manipulator = new ByteArrayBitsManipulator( MAX_SLOT_BITS, 1 );
        try ( ByteArray array = NumberArrayFactory.HEAP.newByteArray( 2, new byte[MAX_BYTES] ) )
        {
            // when
            put( manipulator, array, 0, -1, 0 );
            put( manipulator, array, 1, -1, -1 );

            // then
            verify( manipulator, array, 0, -1, 0 );
            verify( manipulator, array, 1, -1, -1 );
        }
    }

    private void verify( ByteArrayBitsManipulator manipulator, ByteArray array, long index, long... values )
    {
        for ( int i = 0; i < values.length; i++ )
        {
            assertEquals( values[i], manipulator.get( array, index, i ) );
        }
    }

    private void put( ByteArrayBitsManipulator manipulator, ByteArray array, long index, long... values )
    {
        for ( int i = 0; i < values.length; i++ )
        {
            manipulator.set( array, index, i, values[i] );
        }
    }
}
