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
package org.neo4j.kernel.impl.transaction.log;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import org.neo4j.kernel.impl.api.TestCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;

class TransactionPositionLocatorTest
{
    private final LogEntryReader logEntryReader = mock( LogEntryReader.class );
    private final ReadableClosablePositionAwareChecksumChannel channel = mock( ReadableClosablePositionAwareChecksumChannel.class );
    private final TransactionMetadataCache metadataCache = mock( TransactionMetadataCache.class );

    private final long txId = 42;
    private final LogPosition startPosition = new LogPosition( 1, 128 );

    private final LogEntryStart start = new LogEntryStart( 0, 0, 0, null, startPosition );
    private final LogEntryCommand command = new LogEntryCommand( new TestCommand() );
    private final LogEntryCommit commit = new LogEntryCommit( txId, System.currentTimeMillis(), BASE_TX_CHECKSUM );

    @Test
    void shouldFindTransactionLogPosition() throws IOException
    {
        // given
        final PhysicalLogicalTransactionStore.TransactionPositionLocator locator =
                new PhysicalLogicalTransactionStore.TransactionPositionLocator( txId, logEntryReader );

        when( logEntryReader.readLogEntry( channel ) ).thenReturn( start, command, commit, null );

        // when
        final boolean result = locator.visit( channel );
        final LogPosition position = locator.getAndCacheFoundLogPosition( metadataCache );

        // then
        assertFalse( result );
        assertEquals( startPosition, position );
        verify( metadataCache ).cacheTransactionMetadata(
                txId,
                startPosition,
                commit.getChecksum(),
                commit.getTimeWritten()
        );
    }

    @Test
    void shouldNotFindTransactionLogPosition() throws IOException
    {
        // given
        final PhysicalLogicalTransactionStore.TransactionPositionLocator locator =
                new PhysicalLogicalTransactionStore.TransactionPositionLocator( txId, logEntryReader );

        when( logEntryReader.readLogEntry( channel ) ).thenReturn( start, command, null );

        // when
        final boolean result = locator.visit( channel );

        // then
        assertTrue( result );
        NoSuchTransactionException exception =
                assertThrows( NoSuchTransactionException.class, () -> locator.getAndCacheFoundLogPosition( metadataCache ) );
        assertEquals( "Unable to find transaction " + txId + " in any of my logical logs", exception.getMessage() );
    }
}
