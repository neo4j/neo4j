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

import org.junit.Test;

import java.io.IOException;

import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransactionPositionLocatorTest
{
    private final LogEntryReader<ReadableClosablePositionAwareChannel> logEntryReader = mock( LogEntryReader.class );
    private final ReadableClosablePositionAwareChannel channel = mock( ReadableClosablePositionAwareChannel.class );
    private final TransactionMetadataCache metadataCache = mock( TransactionMetadataCache.class );

    private final long txId = 42;
    private final LogPosition startPosition = new LogPosition( 1, 128 );

    private final LogEntryStart start = new LogEntryStart( 0, 0, 0, 0, null, startPosition );
    private final LogEntryCommand command = new LogEntryCommand(
            new Command.NodeCommand( new NodeRecord( 42 ), new NodeRecord( 42 ) ) );
    private final LogEntryCommit commit = new LogEntryCommit( txId, System.currentTimeMillis() );

    @Test
    public void shouldFindTransactionLogPosition() throws IOException
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
        verify( metadataCache, times( 1 ) ).cacheTransactionMetadata(
                txId,
                startPosition,
                start.getMasterId(),
                start.getLocalId(),
                LogEntryStart.checksum( start ),
                commit.getTimeWritten()
        );
    }

    @Test
    public void shouldNotFindTransactionLogPosition() throws IOException
    {
        // given
        final PhysicalLogicalTransactionStore.TransactionPositionLocator locator =
                new PhysicalLogicalTransactionStore.TransactionPositionLocator( txId, logEntryReader );

        when( logEntryReader.readLogEntry( channel ) ).thenReturn( start, command, null );

        // when
        final boolean result = locator.visit( channel );

        // then
        assertTrue( result );
        try
        {
            locator.getAndCacheFoundLogPosition( metadataCache );
            fail( "should have thrown" );
        }
        catch ( NoSuchTransactionException e )
        {
            assertEquals( "Unable to find transaction " + txId + " in any of my logical logs", e.getMessage() );
        }
    }
}
