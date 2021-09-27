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
package org.neo4j.kernel.api;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.List;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Node;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.kernel.api.database.transaction.LogChannel;
import org.neo4j.kernel.api.database.transaction.TransactionLogChannels;
import org.neo4j.kernel.api.database.transaction.TransactionLogService;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFile;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.MetadataProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@DbmsExtension( configurationCallback = "configure" )
class TransactionLogServiceIT
{
    private static final long THRESHOLD = kibiBytes( 128 );

    @Inject
    private GraphDatabaseAPI databaseAPI;
    @Inject
    private TransactionLogService logService;
    @Inject
    private LogFiles logFiles;
    @Inject
    private CheckPointer checkPointer;
    @Inject
    private LogicalTransactionStore transactionStore;
    @Inject
    private MetadataProvider metadataProvider;

    @ExtensionCallback
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( GraphDatabaseSettings.logical_log_rotation_threshold, THRESHOLD )
               .setConfig( GraphDatabaseSettings.keep_logical_logs, "1 files" );
    }

    @Test
    void rotationDuringTransactionLogReadingKeepNonAffectedChannelsOpen() throws IOException
    {
        var propertyValue = RandomStringUtils.randomAscii( (int) THRESHOLD );

        // execute test transaction to create any tokens to avoid tx ids tricks
        executeTransaction( "any" );
        int numberOfTransactions = 30;
        long lastCommittedBeforeWorkload = metadataProvider.getLastCommittedTransactionId();
        for ( int i = 0; i < numberOfTransactions; i++ )
        {
            executeTransaction( propertyValue );
        }

        try ( TransactionLogChannels logReaders = logService.logFilesChannels( lastCommittedBeforeWorkload + 29 ) )
        {
            List<LogChannel> logFileChannels = logReaders.getChannels();
            assertThat( logFileChannels ).hasSize( 3 );
            assertThat( logFiles.logFiles() ).hasSizeGreaterThanOrEqualTo( numberOfTransactions );

            checkPointer.forceCheckPoint( new SimpleTriggerInfo( "Test checkpoint" ) );

            // 1 desired, 1 checkpoint  + 2 (see how ThresholdBasedPruneStrategy is working - keeping additional 2 files)
            assertThat( logFiles.logFiles() ).hasSize( 4 );

            for ( LogChannel logChannel : logFileChannels )
            {
                StoreChannel channel = logChannel.getChannel();
                assertTrue( channel.isOpen() );
                assertDoesNotThrow( channel::size );
            }
        }
    }

    @Test
    void rotationDuringTransactionLogReading() throws IOException
    {
        var propertyValue = RandomStringUtils.randomAscii( (int) THRESHOLD );

        int numberOfTransactions = 30;
        for ( int i = 0; i < numberOfTransactions; i++ )
        {
            executeTransaction( propertyValue );
        }

        try ( TransactionLogChannels logReaders = logService.logFilesChannels( 2 ) )
        {
            List<LogChannel> logFileChannels = logReaders.getChannels();
            assertThat( logFileChannels ).hasSizeGreaterThanOrEqualTo( numberOfTransactions );
            assertThat( logFiles.logFiles() ).hasSizeGreaterThanOrEqualTo( numberOfTransactions );

            checkPointer.forceCheckPoint( new SimpleTriggerInfo( "Test checkpoint" ) );

            // 1 desired, 1 checkpoint  + 2 (see how ThresholdBasedPruneStrategy is working - keeping additional 2 files)
            int txLogsAfterCheckpoint = 3;
            int checkpointLogs = 1;
            assertThat( logFiles.logFiles() ).hasSize( txLogsAfterCheckpoint + checkpointLogs );

            var closedChannels = logFileChannels.subList( 0, logFileChannels.size() - txLogsAfterCheckpoint );
            var openChannels = logFileChannels.subList( logFileChannels.size() - txLogsAfterCheckpoint, logFileChannels.size() );
            assertEquals( closedChannels.size() + openChannels.size(), logFileChannels.size() );

            for ( LogChannel logChannel : closedChannels )
            {
                StoreChannel channel = logChannel.getChannel();
                assertFalse( channel.isOpen() );
                assertThrows( ClosedChannelException.class, channel::size );
            }
            for ( LogChannel logChannel : openChannels )
            {
                StoreChannel channel = logChannel.getChannel();
                assertTrue( channel.isOpen() );
                assertDoesNotThrow( channel::size );
            }
        }
    }

    @Test
    void closingReadersDoesAutomaticCleanup() throws Exception
    {
        var propertyValue = RandomStringUtils.randomAscii( (int) THRESHOLD );

        int numberOfTransactions = 30;
        for ( int i = 0; i < numberOfTransactions; i++ )
        {
            executeTransaction( propertyValue );
        }

        try ( TransactionLogChannels logReaders = logService.logFilesChannels( 2 ) )
        {
            List<LogChannel> logFileChannels = logReaders.getChannels();
            assertThat( logFileChannels ).hasSizeGreaterThanOrEqualTo( numberOfTransactions );
            assertThat( logFiles.logFiles() ).hasSizeGreaterThanOrEqualTo( numberOfTransactions );

            assertThat( ((TransactionLogFile) logFiles.getLogFile()).getExternalFileReaders() ).isNotEmpty();
        }

        assertThat( ((TransactionLogFile) logFiles.getLogFile()).getExternalFileReaders() ).isEmpty();
    }

    @Test
    void requestLogFileChannelsOfInvalidTransactions()
    {
        assertThrows( IllegalArgumentException.class, () -> logService.logFilesChannels( -1 ) );
        assertThrows( IllegalArgumentException.class, () -> logService.logFilesChannels( 100 ) );
    }

    @Test
    void logFileChannelsAreNonWritable() throws IOException
    {
        executeTransaction( "a" );
        executeTransaction( "b" );
        executeTransaction( "c" );

        try ( TransactionLogChannels logReaders = logService.logFilesChannels( 2 ) )
        {
            List<LogChannel> logFileChannels = logReaders.getChannels();
            assertThat( logFileChannels ).hasSize( 1 );

            LogChannel channel = logFileChannels.get( 0 );
            assertEquals( 2, channel.getLastCommittedTxId() );

            StoreChannel storeChannel = channel.getChannel();
            assertThrows( UnsupportedOperationException.class, () -> storeChannel.writeAll( ByteBuffers.allocate( 1, INSTANCE ) ) );
            assertThrows( UnsupportedOperationException.class, () -> storeChannel.writeAll( ByteBuffers.allocate( 1, INSTANCE ), 1 ) );
            assertThrows( UnsupportedOperationException.class, () -> storeChannel.truncate( 1 ) );
            assertThrows( UnsupportedOperationException.class, () -> storeChannel.write( ByteBuffers.allocate( 1, INSTANCE ) ) );
            assertThrows( UnsupportedOperationException.class, () -> storeChannel.write( new ByteBuffer[]{}, 1, 1 ) );
            assertThrows( UnsupportedOperationException.class, () -> storeChannel.write( new ByteBuffer[]{} ) );
        }
    }

    @Test
    void logFileHeaderIdReportedForChannelExceptFirst() throws IOException
    {
        var propertyValue = RandomStringUtils.randomAscii( (int) THRESHOLD / 2 );

        int numberOfTransactions = 40;
        for ( int i = 0; i < numberOfTransactions; i++ )
        {
            executeTransaction( propertyValue );
        }

        int initialTxId = 17;
        try ( TransactionLogChannels logReaders = logService.logFilesChannels( initialTxId ) )
        {
            List<LogChannel> logFileChannels = logReaders.getChannels();
            assertThat( logFileChannels ).hasSize( 15 );

            var channelIterator = logFileChannels.iterator();
            assertEquals( initialTxId, channelIterator.next().getLastCommittedTxId() );
            int subsequentTxId = 18;
            while ( channelIterator.hasNext() )
            {
                assertEquals( subsequentTxId, channelIterator.next().getLastCommittedTxId() );
                subsequentTxId += 2;
            }
        }
    }

    @Test
    void firstLogChannelIsProperlyPositionedToInitialTransaction() throws IOException
    {
        int numberOfTransactions = 20;
        for ( int i = 0; i < numberOfTransactions; i++ )
        {
            executeTransaction( "abc" );
        }

        verifyReportedPositions( 2, getTxOffset( 2 ) );
        verifyReportedPositions( 3, getTxOffset( 3 ) );
        verifyReportedPositions( 4, getTxOffset( 4 ) );
        verifyReportedPositions( 5, getTxOffset( 5 ) );
        verifyReportedPositions( 15, getTxOffset( 15 ) );
    }

    private long getTxOffset( int txId ) throws IOException
    {
        return transactionStore.getTransactions( txId ).position().getByteOffset();
    }

    private void verifyReportedPositions( int txId, long expectedOffset ) throws IOException
    {
        try ( TransactionLogChannels logReaders = logService.logFilesChannels( txId ) )
        {
            List<LogChannel> logFileChannels = logReaders.getChannels();
            assertThat( logFileChannels ).hasSize( 1 );
            assertEquals( expectedOffset, logFileChannels.get( 0 ).getChannel().position() );
        }
    }

    private void executeTransaction( String propertyValue )
    {
        try ( var tx = databaseAPI.beginTx() )
        {
            Node node = tx.createNode();
            node.setProperty( "a", propertyValue );
            tx.commit();
        }
    }
}
