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
package org.neo4j.memory;

import org.github.jamm.MemoryMeter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.memory.HeapEstimator.OBJECT_ALIGNMENT_BYTES;

class HeapEstimatorTest
{
    private final MemoryMeter memoryMeter = new MemoryMeter();

    @Test
    void alignObjectSize()
    {
        for ( int i = 0; i <= 1024; i++ )
        {
            long aligned = HeapEstimator.alignObjectSize( i );
            assertEquals( 0, aligned % OBJECT_ALIGNMENT_BYTES );
        }
    }

    @ParameterizedTest
    @ValueSource( strings = {"", "a", "aa", "aaaΩa"} )
    void stringSize( String s )
    {
        assertEquals( memoryMeter.measureDeep( s ), HeapEstimator.sizeOf( s ) );
    }

    @Test
    void longStringSize()
    {
        String longString = "a".repeat( 5000 ); // Will be compressed
        assertEquals( memoryMeter.measureDeep( longString ), HeapEstimator.sizeOf( longString ) );
        longString = "Ω".repeat( 5000 ); // Will not be compressed
        assertEquals( memoryMeter.measureDeep( longString ), HeapEstimator.sizeOf( longString ) );
    }
}
