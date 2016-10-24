/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.coreedge.core.consensus.log;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

import static org.neo4j.coreedge.core.consensus.ReplicatedInteger.valueOf;
import static org.neo4j.coreedge.core.consensus.log.RaftLogHelper.readLogEntry;

public class BoundedArrayBackedRaftLogTest
{
    @Test
    public void shouldInitiallyBeEmpty() throws Exception
    {
        // given
        RaftLog log = new BoundedArrayBackedRaftLog( 10 );

        // then
        assertEquals( -1L, log.prevIndex() );
        assertEquals( -1L, log.appendIndex() );
    }

    @Test
    public void shouldStoreEntries() throws Exception
    {
        // given
        RaftLog log = new BoundedArrayBackedRaftLog( 10 );

        // when
        log.append( entries( 5 ) );

        // then
        assertEquals( -1L, log.prevIndex() );
        assertEquals( 4L, log.appendIndex() );
        assertEquals( valueOf( 0 ), readLogEntry( log, 0 ).content() );
        assertEquals( valueOf( 4 ), readLogEntry( log, 4 ).content() );
    }

    @Test
    public void shouldSkipBeyondMaxInt() throws Exception
    {
        // given
        RaftLog log = new BoundedArrayBackedRaftLog( 10 );

        // when
        log.skip( Integer.MAX_VALUE + 13L, 0L );
        assertEquals( Integer.MAX_VALUE + 13L, log.prevIndex() );
        assertEquals( Integer.MAX_VALUE + 13L, log.appendIndex() );

        log.append( entries( 5 ) );

        // then
        assertEquals( Integer.MAX_VALUE + 13L, log.prevIndex() );
        assertEquals( Integer.MAX_VALUE + 18L, log.appendIndex() );
        assertEquals( valueOf( 0 ), readLogEntry( log, Integer.MAX_VALUE + 14L ).content() );
        assertEquals( valueOf( 4 ), readLogEntry( log, Integer.MAX_VALUE + 18L ).content() );
    }

    @Test
    public void shouldPruneWhenCapacityExceeded() throws Exception
    {
        // given
        RaftLog log = new BoundedArrayBackedRaftLog( 10 );

        // when
        log.append( entries( 21 ) );

        // then
        assertEquals( 10L, log.prevIndex() );
        assertEquals( 20L, log.appendIndex() );
        assertEquals( valueOf( 11 ), readLogEntry( log, 11 ).content() );
        assertEquals( valueOf( 20 ), readLogEntry( log, 20 ).content() );
    }

    private static RaftLogEntry[] entries( int count )
    {
        RaftLogEntry[] entries = new RaftLogEntry[count];
        for ( int i = 0; i < count; i++ )
        {
            entries[i] = new RaftLogEntry( 0, valueOf( i ) );
        }
        return entries;
    }
}