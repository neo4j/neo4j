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
package org.neo4j.coreedge.raft.log;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.neo4j.kernel.impl.transaction.log.LogPosition;

public class RaftLogMetadataCacheTest
{
    @Test
    public void shouldReturnNullWhenMissingAnEntryInTheCache()
    {
        // given
        final RaftLogMetadataCache cache = new RaftLogMetadataCache( 2 );

        // when
        final RaftLogMetadataCache.RaftLogEntryMetadata metadata = cache.getMetadata( 42 );

        // then
        assertNull( metadata );
    }

    @Test
    public void shouldReturnTheTxValueTIfInTheCached()
    {
        // given
        final RaftLogMetadataCache cache = new RaftLogMetadataCache( 2 );
        final long index = 12L;
        final long term = 12L;
        final LogPosition position = new LogPosition( 3, 4 );

        // when
        cache.cacheMetadata( index, term, position );
        final RaftLogMetadataCache.RaftLogEntryMetadata metadata = cache.getMetadata( index );

        // then
        assertEquals( new RaftLogMetadataCache.RaftLogEntryMetadata( term, position ), metadata );
    }

    @Test
    public void shouldClearTheCache()
    {
        // given
        final RaftLogMetadataCache cache = new RaftLogMetadataCache( 2 );
        final long index = 12L;
        final long term = 12L;
        final LogPosition position = new LogPosition( 3, 4 );

        // when
        cache.cacheMetadata( index, term, position );
        cache.clear();
        RaftLogMetadataCache.RaftLogEntryMetadata metadata = cache.getMetadata( index );

        // then
        assertNull( metadata );
    }

}