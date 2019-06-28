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
package org.neo4j.kernel.impl.transaction;

import org.junit.jupiter.api.Test;

import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.TransactionMetadataCache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TransactionMetadataCacheTest
{
    @Test
    void shouldReturnNullWhenMissingATxInTheCache()
    {
        // given
        final TransactionMetadataCache cache = new TransactionMetadataCache();

        // when
        final TransactionMetadataCache.TransactionMetadata metadata = cache.getTransactionMetadata( 42 );

        // then
        assertNull( metadata );
    }

    @Test
    void shouldReturnTheTxValueTIfInTheCached()
    {
        // given
        final TransactionMetadataCache cache = new TransactionMetadataCache();
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
    void shouldThrowWhenCachingATxWithNegativeOffsetPosition()
    {
        // given
        final TransactionMetadataCache cache = new TransactionMetadataCache();
        final LogPosition position = new LogPosition( 3, -1 );
        final int txId = 42;
        final int masterId = 0;
        final int authorId = 1;
        final int checksum = 2;
        final long timestamp = System.currentTimeMillis();

        var e = assertThrows( RuntimeException.class, () -> cache.cacheTransactionMetadata( txId, position, masterId, authorId, checksum, timestamp ) );
        assertEquals( "StartEntry.position is " + position, e.getMessage() );
    }

    @Test
    void shouldClearTheCache()
    {
        // given
        final TransactionMetadataCache cache = new TransactionMetadataCache();
        final LogPosition position = new LogPosition( 3, 4 );
        final int txId = 42;
        final int masterId = 0;
        final int authorId = 1;
        final int checksum = 2;
        final long timestamp = System.currentTimeMillis();

        // when
        cache.cacheTransactionMetadata( txId, position, masterId, authorId, checksum, timestamp );
        cache.clear();
        final TransactionMetadataCache.TransactionMetadata metadata = cache.getTransactionMetadata( txId );

        // then
        assertNull( metadata );
    }
}
