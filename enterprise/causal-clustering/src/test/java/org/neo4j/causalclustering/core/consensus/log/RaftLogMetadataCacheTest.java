/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.core.consensus.log;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

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

    @Test
    public void shouldRemoveUpTo()
    {
        // given
        int cacheSize = 100;
        RaftLogMetadataCache cache = new RaftLogMetadataCache( cacheSize );

        for ( int i = 0; i < cacheSize; i++ )
        {
            cache.cacheMetadata( i, i, new LogPosition( i, i ) );
        }

        // when
        int upTo = 30;
        cache.removeUpTo( upTo );

        // then
        long i = 0;
        for ( ; i <= upTo; i++ )
        {
            assertNull( cache.getMetadata( i ) );
        }
        for ( ; i < cacheSize; i++ )
        {
            RaftLogMetadataCache.RaftLogEntryMetadata metadata = cache.getMetadata( i );
            assertNotNull( metadata );
            assertEquals( i, metadata.getEntryTerm() );
        }
    }

    @Test
    public void shouldRemoveUpwardsFrom()
    {
        // given
        int cacheSize = 100;
        RaftLogMetadataCache cache = new RaftLogMetadataCache( cacheSize );

        for ( int i = 0; i < cacheSize; i++ )
        {
            cache.cacheMetadata( i, i, new LogPosition( i, i ) );
        }

        // when
        int upFrom = 60;
        cache.removeUpwardsFrom( upFrom );

        // then
        long i = 0;
        for ( ; i < upFrom; i++ )
        {
            RaftLogMetadataCache.RaftLogEntryMetadata metadata = cache.getMetadata( i );
            assertNotNull( metadata );
            assertEquals( i, metadata.getEntryTerm() );
        }
        for ( ; i < cacheSize; i++ )
        {
            assertNull( cache.getMetadata( i ) );
        }
    }

    @Test
    public void shouldAcceptAndReturnIndexesInRangeJustDeleted()
    {
        // given
        int cacheSize = 100;
        RaftLogMetadataCache cache = new RaftLogMetadataCache( cacheSize );

        for ( int i = 0; i < cacheSize; i++ )
        {
            cache.cacheMetadata( i, i, new LogPosition( i, i ) );
        }

        // when
        int upFrom = 60;
        cache.removeUpwardsFrom( upFrom );

        // and we add something in the deleted range
        int insertedIndex = 70;
        long insertedTerm = 150;
        cache.cacheMetadata( insertedIndex, insertedTerm, new LogPosition( insertedIndex, insertedIndex ) );

        // then
        // nothing should be resurrected in the deleted range just because we inserted something there
        int i = upFrom;
        for ( ; i < insertedIndex; i++ )
        {
            assertNull( cache.getMetadata( i ) );
        }
        // i here should be insertedIndex
        assertEquals( insertedTerm, cache.getMetadata( i ).getEntryTerm() );
        i++; // to continue iteration in the rest of the deleted range
        for (; i < cacheSize; i++ )
        {
            assertNull( cache.getMetadata( i ) );
        }
    }
}
