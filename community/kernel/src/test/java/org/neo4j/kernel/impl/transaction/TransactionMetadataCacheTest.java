/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction;

import org.junit.Test;

import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.TransactionMetadataCache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class TransactionMetadataCacheTest
{
    @Test
    public void shouldReturnNullWhenMissingATxInTheCache()
    {
        // given
        final TransactionMetadataCache cache = new TransactionMetadataCache( 2, 2 );

        // when
        final TransactionMetadataCache.TransactionMetadata metadata = cache.getTransactionMetadata( 42 );

        // then
        assertNull( metadata );
    }

    @Test
    public void shouldReturnTheTxValueTIfInTheCached()
    {
        // given
        final TransactionMetadataCache cache = new TransactionMetadataCache( 2, 2 );
        final LogPosition position = new LogPosition( 3, 4 );
        final int txId = 42;
        final int masterId = 0;
        final int authorId = 1;
        final int checksum = 2;
        final long timestamp = System.currentTimeMillis();

        // when
        cache.cacheTransactionMetadata( txId, position, masterId, authorId, checksum, timestamp );
        final TransactionMetadataCache.TransactionMetadata metadata = cache.getTransactionMetadata( txId );

        // then
        assertEquals(
                new TransactionMetadataCache.TransactionMetadata( masterId, authorId, position, checksum, timestamp ),
                metadata );
    }

    @Test
    public void shouldThrowWhenCachingATxWithNegativeOffsetPosition()
    {
        // given
        final TransactionMetadataCache cache = new TransactionMetadataCache( 2, 2 );
        final LogPosition position = new LogPosition( 3, -1 );
        final int txId = 42;
        final int masterId = 0;
        final int authorId = 1;
        final int checksum = 2;
        final long timestamp = System.currentTimeMillis();

        // when
        try
        {
            cache.cacheTransactionMetadata( txId, position, masterId, authorId, checksum, timestamp );
            fail();
        } catch (RuntimeException ex) {
            assertEquals( "StartEntry.position is " + position, ex.getMessage() );
        }
    }

    @Test
    public void shouldReturnNegativeNumberWhenThereIsNoHeaderInTheCache()
    {
        // given
        final TransactionMetadataCache cache = new TransactionMetadataCache( 2, 2 );

        // when
        final long logHeader = cache.getLogHeader( 5 );

        // then
        assertEquals( -1, logHeader );
    }

    @Test
    public void shouldReturnTheHeaderIfInTheCache()
    {
        // given
        final TransactionMetadataCache cache = new TransactionMetadataCache( 2, 2 );

        // when
        cache.putHeader( 5, 3 );
        final long logHeader = cache.getLogHeader( 5 );

        // then
        assertEquals( 3, logHeader );
    }

    @Test
    public void shouldClearTheCaches()
    {
        // given
        final TransactionMetadataCache cache = new TransactionMetadataCache( 2, 2 );
        final LogPosition position = new LogPosition( 3, 4 );
        final int txId = 42;
        final int masterId = 0;
        final int authorId = 1;
        final int checksum = 2;
        final long timestamp = System.currentTimeMillis();

        // when
        cache.cacheTransactionMetadata( txId, position, masterId, authorId, checksum, timestamp );
        cache.putHeader( 5, 3 );
        cache.clear();
        final long logHeader = cache.getLogHeader( 5 );
        final TransactionMetadataCache.TransactionMetadata metadata = cache.getTransactionMetadata( txId );

        // then
        assertEquals( -1, logHeader );
        assertNull( metadata );
    }
}
