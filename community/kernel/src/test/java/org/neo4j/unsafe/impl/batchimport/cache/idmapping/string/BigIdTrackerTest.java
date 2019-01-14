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

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.test.rule.RandomRule;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;

import static org.junit.Assert.assertEquals;
import static org.neo4j.unsafe.impl.batchimport.cache.idmapping.string.BigIdTracker.MAX_ID;

public class BigIdTrackerTest
{
    @Rule
    public final RandomRule random = new RandomRule();

    @Test
    public void shouldKeepIdsAndMarkDuplicates()
    {
        // given
        int length = 10_000;
        try ( BigIdTracker tracker = new BigIdTracker( NumberArrayFactory.HEAP.newByteArray( length, BigIdTracker.DEFAULT_VALUE ) ) )
        {
            // when
            long[] values = new long[length];
            boolean[] marks = new boolean[length];
            for ( int i = 0; i < length; i++ )
            {
                tracker.set( i, values[i] = random.nextLong( MAX_ID ) );
                if ( random.nextBoolean() )
                {
                    tracker.markAsDuplicate( i );
                    marks[i] = true;
                }
            }

            // then
            for ( int i = 0; i < length; i++ )
            {
                assertEquals( values[i], tracker.get( i ) );
                assertEquals( marks[i], tracker.isMarkedAsDuplicate( i ) );
            }
        }
    }
}
