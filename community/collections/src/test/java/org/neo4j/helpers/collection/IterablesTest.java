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
package org.neo4j.helpers.collection;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.neo4j.function.ThrowingConsumer;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class IterablesTest
{
    @Test
    void safeForAllShouldConsumeAllSubjectsRegardlessOfSuccess()
    {
        // given
        List<String> seenSubjects = new ArrayList<>();
        List<String> failedSubjects = new ArrayList<>();
        ThrowingConsumer<String,RuntimeException> consumer = new ThrowingConsumer<String,RuntimeException>()
        {
            @Override
            public void accept( String s )
            {
                seenSubjects.add( s );

                // Fail every other
                if ( seenSubjects.size() % 2 == 1 )
                {
                    failedSubjects.add( s );
                    throw new RuntimeException( s );
                }
            }
        };
        Iterable<String> subjects = asList( "1", "2", "3", "4", "5" );

        // when
        try
        {
            Iterables.safeForAll( consumer, subjects );
            fail( "Should have thrown exception" );
        }
        catch ( RuntimeException e )
        {
            // then good
            assertEquals( subjects, seenSubjects );
            Iterator<String> failed = failedSubjects.iterator();
            assertTrue( failed.hasNext() );
            assertEquals( e.getMessage(), failed.next() );
            for ( Throwable suppressed : e.getSuppressed() )
            {
                assertTrue( failed.hasNext() );
                assertEquals( suppressed.getMessage(), failed.next() );
            }
            assertFalse( failed.hasNext() );
        }
    }
}
