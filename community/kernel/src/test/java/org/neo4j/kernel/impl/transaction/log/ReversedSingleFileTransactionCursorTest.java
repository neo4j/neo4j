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
package org.neo4j.kernel.impl.transaction.log;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.DeadSimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.lifecycle.LifeRule;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.kernel.impl.store.record.Record.NO_LABELS_FIELD;
import static org.neo4j.kernel.impl.transaction.log.GivenTransactionCursor.exhaust;
import static org.neo4j.kernel.impl.transaction.log.LogPosition.start;
import static org.neo4j.kernel.impl.transaction.log.LogVersionBridge.NO_MORE_CHANNELS;
import static org.neo4j.kernel.impl.transaction.log.PhysicalLogFile.NO_MONITOR;

public class ReversedSingleFileTransactionCursorTest
{
    private final DefaultFileSystemRule fs = new DefaultFileSystemRule();
    private final TestDirectory directory = TestDirectory.testDirectory( fs );
    private final LifeRule life = new LifeRule( true );
    private final RandomRule random = new RandomRule();

    @Rule
    public final RuleChain rules = RuleChain.outerRule( random ).around( fs ).around( directory ).around( life );

    private long txId = TransactionIdStore.BASE_TX_ID;
    private PhysicalLogFile logFile;

    @Before
    public void setUp()
    {
        LogVersionRepository logVersionRepository = new DeadSimpleLogVersionRepository( 0 );
        logFile = life.add( new PhysicalLogFile( fs, new PhysicalLogFiles( directory.absolutePath(), fs ), mebiBytes( 1 ),
                () -> txId, logVersionRepository, NO_MONITOR, new LogHeaderCache( 10 ) ) );
    }

    @Test
    public void shouldHandleVerySmallTransactions() throws Exception
    {
        // given
        writeTransactions( 10, 1, 1 );

        // when
        CommittedTransactionRepresentation[] readTransactions = readAllFromReversedCursor();

        // then
        assertTransactionRange( readTransactions, txId, TransactionIdStore.BASE_TX_ID );
    }

    @Test
    public void shouldHandleManyVerySmallTransactions() throws Exception
    {
        // given
        writeTransactions( 20_000, 1, 1 );

        // when
        CommittedTransactionRepresentation[] readTransactions = readAllFromReversedCursor();

        // then
        assertTransactionRange( readTransactions, txId, TransactionIdStore.BASE_TX_ID );
    }

    @Test
    public void shouldHandleLargeTransactions() throws Exception
    {
        // given
        writeTransactions( 10, 1000, 1000 );

        // when
        CommittedTransactionRepresentation[] readTransactions = readAllFromReversedCursor();

        // then
        assertTransactionRange( readTransactions, txId, TransactionIdStore.BASE_TX_ID );
    }

    @Test
    public void shouldHandleEmptyLog() throws Exception
    {
        // given

        // when
        CommittedTransactionRepresentation[] readTransactions = readAllFromReversedCursor();

        // then
        assertEquals( 0, readTransactions.length );
    }

    @Test
    public void shouldDetectAndPreventChannelReadingMultipleLogVersions() throws Exception
    {
        // given
        writeTransactions( 1, 1, 1 );
        logFile.rotate();
        writeTransactions( 1, 1, 1 );

        // when
        try ( ReadAheadLogChannel channel = (ReadAheadLogChannel) logFile.getReader( start( 0 ) ) )
        {
            new ReversedSingleFileTransactionCursor( channel, new VersionAwareLogEntryReader<>() );
            fail( "Should've failed" );
        }
        catch ( IllegalArgumentException e )
        {
            // then good
            assertThat( e.getMessage(), containsString( "multiple log versions" ) );
        }
    }

    private CommittedTransactionRepresentation[] readAllFromReversedCursor() throws IOException
    {
        try ( ReversedSingleFileTransactionCursor cursor = txCursor() )
        {
            return exhaust( cursor );
        }
    }

    private void assertTransactionRange( CommittedTransactionRepresentation[] readTransactions, long highTxId, long lowTxId )
    {
        long expectedTxId = highTxId;
        for ( int i = 0; i < readTransactions.length; i++ )
        {
            CommittedTransactionRepresentation tx = readTransactions[i];
            assertEquals( expectedTxId, tx.getCommitEntry().getTxId() );
            expectedTxId--;
        }
        assertEquals( expectedTxId, lowTxId );
    }

    private ReversedSingleFileTransactionCursor txCursor() throws IOException
    {
        return new ReversedSingleFileTransactionCursor(
                (ReadAheadLogChannel) logFile.getReader( start( 0 ), NO_MORE_CHANNELS ), new VersionAwareLogEntryReader<>() );
    }

    private void writeTransactions( int transactionCount, int minTransactionSize, int maxTransactionSize ) throws IOException
    {
        FlushablePositionAwareChannel channel = logFile.getWriter();
        TransactionLogWriter writer = new TransactionLogWriter( new LogEntryWriter( channel ) );
        for ( int i = 0; i < transactionCount; i++ )
        {
            writer.append( tx( random.intBetween( minTransactionSize, maxTransactionSize ) ), ++txId );
        }
        channel.prepareForFlush().flush();
        // Don't close the channel, LogFile owns it
    }

    private TransactionRepresentation tx( int size )
    {
        Collection<StorageCommand> commands = new ArrayList<>();
        for ( int i = 0; i < size; i++ )
        {
            // The type of command doesn't matter here
            commands.add( new Command.NodeCommand(
                    new NodeRecord( i ),
                    new NodeRecord( i ).initialize( true, i, false, i, NO_LABELS_FIELD.longValue() ) ) );
        }
        PhysicalTransactionRepresentation tx = new PhysicalTransactionRepresentation( commands );
        tx.setHeader( new byte[0], 0, 0, 0, 0, 0, 0 );
        return tx;
    }
}
