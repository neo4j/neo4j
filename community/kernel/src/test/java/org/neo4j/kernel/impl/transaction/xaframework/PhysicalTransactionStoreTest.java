/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.xaframework;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.neo4j.kernel.impl.transaction.xaframework.LogPruneStrategies.NO_PRUNING;
import static org.neo4j.kernel.impl.transaction.xaframework.PhysicalLogFile.DEFAULT_NAME;
import static org.neo4j.kernel.impl.util.Providers.singletonProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.TransactionIdStore;
import org.neo4j.kernel.impl.nioneo.xa.XaCommandReaderFactory;
import org.neo4j.kernel.impl.nioneo.xa.command.Command;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TargetDirectory.TestDirectory;

public class PhysicalTransactionStoreTest
{
    @Test
    public void shouldOpenCleanStore() throws Exception
    {
        // GIVEN
        TransactionIdStore transactionIdStore = new DeadSimpleTransactionIdStore( 0l );
        LogRotationControl logRotationControl = mock( LogRotationControl.class );
        LogPositionCache positionCache = new LogPositionCache( 10, 1000 );

        LogFile logFile = new PhysicalLogFile( fs, directory.directory(), DEFAULT_NAME, 1000, NO_PRUNING,
                transactionIdStore, mock( LogVersionRepository.class), new Monitors().newMonitor( PhysicalLogFile.Monitor.class ), logRotationControl,
                positionCache );
        TxIdGenerator txIdGenerator = new DefaultTxIdGenerator( singletonProvider( transactionIdStore ) );
        try ( TransactionStore store = new PhysicalTransactionStore( logFile, txIdGenerator, positionCache,
                new VersionAwareLogEntryReader( XaCommandReaderFactory.DEFAULT ) ) )
        {
            // WHEN
            store.open( new Visitor<TransactionRepresentation, IOException>()
            {
                @Override
                public boolean visit( TransactionRepresentation transaction ) throws IOException
                {
                    fail( "Should be nothing to recover" );
                    return false;
                }
            } );
        }
    }

    @Test
    public void shouldOpenAndRecoverExistingData() throws Exception
    {
        // GIVEN
        InMemoryLogChannel channel = new InMemoryLogChannel();
        TransactionIdStore transactionIdStore = new DeadSimpleTransactionIdStore( 0l );
        TxIdGenerator txIdGenerator = new DefaultTxIdGenerator( singletonProvider( transactionIdStore ) );
        LogPositionCache positionCache = new LogPositionCache( 10, 100 );
        final byte[] additionalHeader = new byte[] {1, 2, 5};
        final int masterId = 2, authorId = 1;
        final long timeWritten = 12345, latestCommittedTxWhenStarted = 4545;
        addATransactionAndRewind( channel, txIdGenerator, positionCache,
                additionalHeader, masterId, authorId, timeWritten, latestCommittedTxWhenStarted );
        LogFile logFile = new InMemoryLogFile( channel );
        final AtomicInteger recoveredTransactions = new AtomicInteger();

        // WHEN
        try ( TransactionStore store = new PhysicalTransactionStore( logFile, txIdGenerator, positionCache,
                new VersionAwareLogEntryReader( XaCommandReaderFactory.DEFAULT ) ) )
        {
            store.open( new Visitor<TransactionRepresentation, IOException>()
            {
                @Override
                public boolean visit( TransactionRepresentation transaction ) throws IOException
                {
                    assertArrayEquals( additionalHeader, transaction.additionalHeader() );
                    assertEquals( masterId, transaction.getMasterId() );
                    assertEquals( authorId, transaction.getAuthorId() );
                    assertEquals( timeWritten, transaction.getTimeWritten() );
                    assertEquals( latestCommittedTxWhenStarted, transaction.getLatestCommittedTxWhenStarted() );
                    recoveredTransactions.incrementAndGet();
                    return true;
                }
            } );
        }

        // THEN
        assertEquals( 1, recoveredTransactions.get() );
    }

    private void addATransactionAndRewind( InMemoryLogChannel channel, TxIdGenerator txIdGenerator,
            LogPositionCache positionCache, byte[] additionalHeader, int masterId, int authorId, long timeWritten,
            long latestCommittedTxWhenStarted ) throws IOException
    {
        int position = channel.writerPosition();
        try ( TransactionAppender appender = new PhysicalTransactionAppender( channel, txIdGenerator, positionCache ) )
        {
            PhysicalTransactionRepresentation transaction =
                    new PhysicalTransactionRepresentation( singleCreateNodeCommand(), false );
            transaction.setHeader( additionalHeader, masterId, authorId, timeWritten, latestCommittedTxWhenStarted );
            appender.append( transaction );
        }
        finally
        {
            channel.positionWriter( position );
        }
    }

    private Collection<Command> singleCreateNodeCommand()
    {
        Collection<Command> commands = new ArrayList<>();
        Command.NodeCommand command = new Command.NodeCommand();

        long id = 0;
        NodeRecord before = new NodeRecord( id );
        NodeRecord after = new NodeRecord( id );
        after.setInUse( true );
        command.init( before, after );

        commands.add( command );
        return commands;
    }

    private final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
    public final @Rule TestDirectory directory = TargetDirectory.testDirForTest( getClass() );
}
