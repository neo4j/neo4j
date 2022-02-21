/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.transaction.log.reverse;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.api.TestCommand;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.FlushablePositionAwareChecksumChannel;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.TransactionCursor;
import org.neo4j.kernel.impl.transaction.log.TransactionLogWriter;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.RandomExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.neo4j.internal.kernel.api.security.AuthSubject.ANONYMOUS;
import static org.neo4j.kernel.impl.transaction.log.GivenTransactionCursor.exhaust;
import static org.neo4j.kernel.impl.transaction.log.TestLogEntryReader.logEntryReader;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_FORMAT_LOG_HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.log.reverse.ReversedMultiFileTransactionCursor.fromLogFile;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

@Neo4jLayoutExtension
@ExtendWith( {RandomExtension.class, LifeExtension.class} )
class ReversedMultiFileTransactionCursorTest
{
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private DatabaseLayout databaseLayout;
    @Inject
    private LifeSupport life;
    @Inject
    private RandomSupport random;

    private long txId = BASE_TX_ID;
    private final AssertableLogProvider logProvider = new AssertableLogProvider( true );
    private ReverseTransactionCursorLoggingMonitor monitor;
    private LogFile logFile;
    private LogFiles logFiles;

    @BeforeEach
    void setUp() throws IOException
    {
        LogVersionRepository logVersionRepository = new SimpleLogVersionRepository();
        SimpleTransactionIdStore transactionIdStore = new SimpleTransactionIdStore();
        logFiles = LogFilesBuilder
                .builder( databaseLayout, fs )
                .withLogVersionRepository( logVersionRepository )
                .withTransactionIdStore( transactionIdStore )
                .withCommandReaderFactory( new TestCommandReaderFactory() )
                .withStoreId( StoreId.UNKNOWN )
                .build();
        life.add( logFiles );
        logFile = logFiles.getLogFile();
        monitor = mock( ReverseTransactionCursorLoggingMonitor.class );
    }

    @ParameterizedTest
    @ValueSource( booleans = {true, false} )
    void shouldReadFromSingleVersion( boolean presketch ) throws Exception
    {
        // given
        writeTransactions( 10 );

        // when
        var readTransactions = readTransactions( presketch );

        // then
        assertRecovery( presketch, readTransactions, txId, BASE_TX_ID );
    }

    @ParameterizedTest
    @ValueSource( booleans = {true, false} )
    void shouldReadUptoASpecificStartingPosition( boolean presketch ) throws Exception
    {
        // given
        var position = writeTransactions( 2 );
        writeTransactions( 5 );

        // when
        var readTransactions = readTransactions( position, presketch );

        // then
        assertRecovery( presketch, readTransactions, txId, BASE_TX_ID + 2 );
    }

    @ParameterizedTest
    @ValueSource( booleans = {true, false} )
    void shouldReadMultipleVersions( boolean presketch ) throws Exception
    {
        // given
        writeTransactions( 10 );
        logFile.rotate();
        writeTransactions( 5 );
        logFile.rotate();
        writeTransactions( 2 );

        // when
        var readTransactions = readTransactions( presketch );

        // then
        assertRecovery( presketch, readTransactions, txId, BASE_TX_ID );
    }

    @ParameterizedTest
    @ValueSource( booleans = {true, false} )
    void shouldReadUptoASpecificStartingPositionFromMultipleVersions( boolean presketch ) throws Exception
    {
        // given
        writeTransactions( 10 );
        logFile.rotate();
        var position = writeTransactions( 5 );
        writeTransactions( 2 );
        logFile.rotate();
        writeTransactions( 2 );

        // when
        var readTransactions = readTransactions( position, presketch );

        // then
        assertRecovery( presketch, readTransactions, txId, txId - 4 );
    }

    @ParameterizedTest
    @ValueSource( booleans = {true, false} )
    void shouldHandleEmptyLogsMidStream( boolean presketch ) throws Exception
    {
        // given
        writeTransactions( 10 );
        logFile.rotate();
        logFile.rotate();
        writeTransactions( 2 );

        // when
        var readTransactions = readTransactions( presketch );

        // then
        assertRecovery( presketch, readTransactions, txId, BASE_TX_ID );
    }

    @ParameterizedTest
    @ValueSource( booleans = {true, false} )
    void shouldHandleEmptyTransactions( boolean presketch ) throws Exception
    {
        // when
        var readTransactions = readTransactions( presketch );

        // then
        assertThat( readTransactions ).isEmpty();
    }

    private CommittedTransactionRepresentation[] readTransactions( LogPosition position, boolean presketch ) throws IOException
    {
        try ( TransactionCursor cursor = txCursor( position, presketch ) )
        {
            return exhaust( cursor );
        }
    }

    private CommittedTransactionRepresentation[] readTransactions( boolean presketch ) throws IOException
    {
        return readTransactions( new LogPosition( 0, CURRENT_FORMAT_LOG_HEADER_SIZE ), presketch );
    }

    private void assertRecovery( boolean presketch, CommittedTransactionRepresentation[] readTransactions, long highTxId, long lowTxId )
    {
        if ( presketch )
        {
            verify( monitor ).presketchingTransactionLogs();
        }
        else
        {
            verify( monitor, never() ).presketchingTransactionLogs();
        }
        long expectedTxId = highTxId;
        for ( CommittedTransactionRepresentation tx : readTransactions )
        {
            assertEquals( expectedTxId, tx.getCommitEntry().getTxId() );
            expectedTxId--;
        }
        assertEquals( expectedTxId, lowTxId );
    }

    private TransactionCursor txCursor( LogPosition position, boolean presketch ) throws IOException
    {
        ReadAheadLogChannel fileReader = (ReadAheadLogChannel) logFile.getReader( logFiles.getLogFile().extractHeader( 0 ).getStartPosition() );
        try
        {
            return fromLogFile( logFile, position, logEntryReader(), false, monitor, presketch );
        }
        catch ( Exception e )
        {
            fileReader.close();
            throw e;
        }
    }

    private LogPosition writeTransactions( int count ) throws IOException
    {
        FlushablePositionAwareChecksumChannel channel = logFile.getTransactionLogWriter().getChannel();
        TransactionLogWriter writer = logFile.getTransactionLogWriter();
        int previousChecksum = BASE_TX_CHECKSUM;
        for ( int i = 0; i < count; i++ )
        {
            previousChecksum = writer.append( tx( random.intBetween( 1, 5 ) ), ++txId, previousChecksum );
        }
        channel.prepareForFlush().flush();
        return writer.getCurrentPosition();
    }

    private static TransactionRepresentation tx( int size )
    {
        List<StorageCommand> commands = new ArrayList<>();
        for ( int i = 0; i < size; i++ )
        {
            commands.add( new TestCommand() );
        }
        PhysicalTransactionRepresentation tx = new PhysicalTransactionRepresentation( commands );
        tx.setHeader( new byte[0], 0, 0, 0, 0, ANONYMOUS );
        return tx;
    }
}
