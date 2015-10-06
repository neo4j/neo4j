/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.consistency.checking.full;

import org.junit.Test;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

import static java.util.Arrays.asList;

import static org.neo4j.helpers.progress.ProgressListener.NONE;

public class RecordDistributorTest
{
    /**
     * This test will not deterministically trigger the race which the fix inside {@link RecordDistributor}
     * fixes, but very often. On the other hand the test is fast and will not report false failures either.
     * Over time, as many builds are running this test, correctness will be asserted.
     */
    @Test
    public void shouldProcessFirstAndLastRecordFirstAndLast() throws Exception
    {
        // GIVEN
        final Collection<Integer> records = asList( 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 );
        final int count = records.size();
        final AtomicInteger calls = new AtomicInteger();
        RecordProcessor<Integer> processor = new RecordProcessor<Integer>()
        {
            @Override
            public void process( Integer record )
            {
                int call = calls.getAndIncrement();
                if ( record == 0 || record == count - 1 )
                {
                    assertEquals( record.intValue(), call );
                }
            }

            @Override
            public void close()
            {
            }
        };

        // WHEN
        RecordDistributor.distributeRecords( count, getClass().getSimpleName(), 100, records, NONE, processor );

        // THEN
        assertEquals( count, calls.get() );
    }
}
