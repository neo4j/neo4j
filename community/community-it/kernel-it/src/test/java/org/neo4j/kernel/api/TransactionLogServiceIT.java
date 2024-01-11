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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.file.Path;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Node;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.database.transaction.LogChannel;
import org.neo4j.kernel.api.database.transaction.TransactionLogChannels;
import org.neo4j.kernel.api.database.transaction.TransactionLogService;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.availability.DescriptiveAvailabilityRequirement;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.impl.api.tracer.DefaultTracer;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFile;
import org.neo4j.kernel.impl.transaction.log.rotation.monitor.LogRotationMonitorAdapter;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.impl.transaction.tracing.StoreApplyEvent;
import org.neo4j.kernel.impl.transaction.tracing.TransactionEvent;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.monitoring.tracing.Tracers;
import org.neo4j.lock.LockTracer;
import org.neo4j.logging.NullLog;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.MetadataProvider;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.time.Clocks;
import org.neo4j.util.concurrent.BinaryLatch;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.OptionalLong.empty;
import static org.apache.commons.lang3.RandomStringUtils.randomAscii;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@DbmsExtension( configurationCallback = "configure" )
class TransactionLogServiceIT
{
    private static final long THRESHOLD = kibiBytes( 128 );

    @Inject
    private GraphDatabaseAPI databaseAPI;
    @Inject
    private DatabaseManagementService managementService;
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
    @Inject
    private DatabaseAvailabilityGuard availabilityGuard;

    @ExtensionCallback
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        var dependencies = new Dependencies();
        var tracers = new InjectableBeforeApplyTracers();
        dependencies.satisfyDependency( tracers );
        builder.setExternalDependencies( dependencies );

        builder.setConfig(GraphDatabaseSettings.logical_log_rotation_threshold, THRESHOLD)
                .setConfig(GraphDatabaseSettings.keep_logical_logs, "1 files");
    }

    @Test
    void rotationDuringTransactionLogReadingKeepNonAffectedChannelsOpen() throws IOException
    {
        var propertyValue = randomAscii( (int) THRESHOLD );

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
            assertThat( logFileChannels ).hasSize( 2 );
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
        var propertyValue = randomAscii( (int) THRESHOLD );

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

            // 1 desired, 1 checkpoint + 2 (see how ThresholdBasedPruneStrategy is working - keeping additional 2 files)
            int txLogsAfterCheckpoint = 3;
            // the transaction log service did not return the last (empty) transaction log file
            var visibleTxLogsAfterCheckpoints = txLogsAfterCheckpoint - 1;
            int checkpointLogs = 1;

            assertThat( logFiles.logFiles() ).hasSize( txLogsAfterCheckpoint + checkpointLogs );

            var closedChannels = logFileChannels.subList( 0, logFileChannels.size() - visibleTxLogsAfterCheckpoints );
            var openChannels = logFileChannels.subList( logFileChannels.size() - visibleTxLogsAfterCheckpoints, logFileChannels.size() );
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
        var propertyValue = randomAscii( (int) THRESHOLD );

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
    void requireDirectByteBufferForLogFileAppending()
    {
        availabilityGuard.require( new DescriptiveAvailabilityRequirement( "Database unavailable" ) );

        assertThrows( IllegalArgumentException.class, () -> logService.append( ByteBuffer.allocate( 5 ), empty()  ) );
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
            assertEquals( 2, channel.getStartTxId() );

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
    void setsStartingTransactionIdCorrectlyForAllFiles() throws IOException
    {
        var propertyValue = randomAscii( (int) THRESHOLD / 2 );

        int numberOfTransactions = 40;
        for ( int i = 0; i < numberOfTransactions; i++ )
        {
            executeTransaction( propertyValue );
        }

        int initialTxId = 17;
        try ( TransactionLogChannels logReaders = logService.logFilesChannels( initialTxId ) )
        {
            List<LogChannel> logFileChannels = logReaders.getChannels();
            assertThat( logFileChannels ).hasSize( 14 );

            var channelIterator = logFileChannels.iterator();
            assertEquals( initialTxId, channelIterator.next().getStartTxId() );
            int subsequentTxId = 19;
            while ( channelIterator.hasNext() )
            {
                assertEquals( subsequentTxId, channelIterator.next().getStartTxId() );
                subsequentTxId += 2;
            }
        }
    }

    @Test
    void setsLastTransactionIdCorrectlyForAllFiles() throws IOException
    {
        var propertyValue = randomAscii( (int) THRESHOLD / 2 );

        int numberOfTransactions = 40;
        for ( int i = 0; i < numberOfTransactions; i++ )
        {
            executeTransaction( propertyValue );
        }

        int initialTxId = 17;
        try ( TransactionLogChannels logReaders = logService.logFilesChannels( initialTxId ) )
        {
            List<LogChannel> logFileChannels = logReaders.getChannels();
            assertThat( logFileChannels ).hasSize( 14 );

            var channelIterator = logFileChannels.iterator();
            assertEquals( initialTxId + 1, channelIterator.next().getLastTxId() );
            int subsequentLastTxId = 20;
            while ( channelIterator.hasNext() )
            {
                assertEquals( subsequentLastTxId, channelIterator.next().getLastTxId() );
                subsequentLastTxId += 2;
            }
        }
    }

    @Test
    void endOffsetPositionedToEndOfFile() throws IOException
    {
        var propertyValue = randomAscii( (int) THRESHOLD );

        int numberOfTransactions = 30;
        for ( int i = 0; i < numberOfTransactions; i++ )
        {
            createNodeInIsolatedTransaction( propertyValue );
        }
        try ( TransactionLogChannels logReaders = logService.logFilesChannels( 2 ) )
        {

            var channels = logReaders.getChannels();
            var fullChannels = channels.subList( 0, channels.size() - 1 );

            for ( LogChannel fullChannel : fullChannels )
            {
                assertThat(fullChannel.getEndOffset())
                        .isEqualTo(fullChannel.getChannel().size());
            }
        }
    }

    // This test ensures that we return the use the last committed transaction, not the last closed transaction as
    // the upper bound when we retrieve channels.
    @Test
    void endOffsetPositionedToLastCommittedTransaction() throws Exception
    {
        createNodeInIsolatedTransaction("some prop value");

        var txIsCommitted = new BinaryLatch();
        var canCloseTx = new BinaryLatch();

        var initialLastCommittedTx = metadataProvider.getLastCommittedTransactionId();
        var initialLastClosedTx = metadataProvider.getLastClosedTransactionId();

        try ( OtherThreadExecutor e1 = new OtherThreadExecutor( "tx taking a long time to apply" ) )
        {
            // lock next applying transaction on canCloseTx so that it is committed but not closed
            InjectableBeforeApplyTracers.InjectableBeforeApplyTxWriteEvent.INSTANCE.beforeStoreApply.set( () ->
            {
                txIsCommitted.release();
                canCloseTx.await();
            });

            var openTx = e1.executeDontWait( () ->
            {
                try ( var tx = databaseAPI.beginTx() )
                {
                    tx.createNode();
                    tx.commit();
                }
                return null;
            });

            try
            {
                txIsCommitted.await();

                // then we should have the last committed after the last closed:
                var lastCommittedTransaction = metadataProvider.getLastCommittedTransactionId();
                var lastClosedTx = metadataProvider.getLastClosedTransactionId();
                assertThat( lastClosedTx ).isEqualTo( initialLastClosedTx );
                assertThat( lastCommittedTransaction ).isEqualTo( initialLastCommittedTx + 1 );

                // when we get the channels starting at the last committed transaction:
                try ( TransactionLogChannels logReaders = logService.logFilesChannels( lastCommittedTransaction ) )
                {
                    var channels = logReaders.getChannels();
                    assertThat( channels ).hasSize( 1 );
                    var channel = channels.get( 0 );
                    // they should include only the last committed transaction (not the last closed)
                    assertThat( channel.getLastTxId() ).isEqualTo( lastCommittedTransaction );
                    assertThat( channel.getStartTxId() ).isEqualTo( lastCommittedTransaction );
                    assertThat( channel.getEndOffset() ).isEqualTo( getTxEndOffset(lastCommittedTransaction) );
                }
            }
            finally
            {
                canCloseTx.release();
                openTx.get( 1, TimeUnit.MINUTES );
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

        verifyReportedPositions( 2, getTxStartOffset( 2 ) );
        verifyReportedPositions( 3, getTxStartOffset( 3 ) );
        verifyReportedPositions( 4, getTxStartOffset( 4 ) );
        verifyReportedPositions( 5, getTxStartOffset( 5 ) );
        verifyReportedPositions( 15, getTxStartOffset( 15 ) );
    }

    @Test
    void failBulkAppendOnNonAvailableDatabase()
    {
        assertThrows( IllegalStateException.class, () -> logService.append( ByteBuffer.wrap( new byte[]{1, 2, 3, 4, 5} ), empty() ) );
    }

    @Test
    void bulkAppendToTransactionLogsDoesNotChangeLastCommittedTransactionOffset() throws IOException
    {
        availabilityGuard.require( new DescriptiveAvailabilityRequirement( "Database unavailable" ) );

        var metadataBefore = metadataProvider.getLastClosedTransaction();
        for ( int i = 0; i < 100; i++ )
        {
            logService.append( createBuffer().put( new byte[]{1, 2, 3, 4, 5} ), empty() );
        }

        assertEquals( metadataBefore, metadataProvider.getLastClosedTransaction() );
    }

    @Test
    void bulkAppendWithRotationDoesNotChangeLastClosedMetadata() throws IOException
    {
        availabilityGuard.require( new DescriptiveAvailabilityRequirement( "Database unavailable" ) );

        var metadataBefore = metadataProvider.getLastClosedTransaction();
        long logVersionBefore = metadataProvider.getCurrentLogVersion();

        var appendData = createBuffer().put( randomAscii( (int) (THRESHOLD + 1) ).getBytes( UTF_8 ) );
        int appendIterations = 100;
        for ( int i = 0; i < appendIterations; i++ )
        {
            logService.append( appendData, OptionalLong.of( i ) );
            appendData.rewind();
        }

        assertEquals( metadataBefore, metadataProvider.getLastClosedTransaction() );

        // pruning is also not here since metadata store is not upgraded
        Path[] matchedFiles = logFiles.getLogFile().getMatchedFiles();
        assertThat( matchedFiles ).hasSize( (int) (logVersionBefore + appendIterations) );
    }

    @Test
    void bulkAppendWithRotationUpdatesMetadataProviderLogVersion() throws IOException
    {
        availabilityGuard.require( new DescriptiveAvailabilityRequirement( "Database unavailable" ) );

        long logVersionBefore = metadataProvider.getCurrentLogVersion();

        var appendData = createBuffer().put( randomAscii( (int) (THRESHOLD + 1) ).getBytes( UTF_8 ) );
        int appendIterations = 100;
        for ( int i = 0; i < appendIterations; i++ )
        {
            logService.append( appendData, OptionalLong.of( i ) );
            appendData.rewind();
        }

        var logVersionAfter = metadataProvider.getCurrentLogVersion();
        assertThat( logVersionAfter ).isEqualTo( logVersionBefore + appendIterations - 1 )
                                     .isNotEqualTo( logVersionBefore );
    }

    @Test
    void bulkAppendRotatedLogFilesHaveCorrectSupplierTransactionsFromHeader() throws IOException
    {
        availabilityGuard.require( new DescriptiveAvailabilityRequirement( "Database unavailable" ) );

        long logVersionBefore = metadataProvider.getCurrentLogVersion();

        var appendData = createBuffer().put( randomAscii( (int) (THRESHOLD + 1) ).getBytes( UTF_8 ) );
        int appendIterations = 100;
        int transactionalShift = 10;
        for ( int i = 0; i < appendIterations; i++ )
        {
            logService.append( appendData, OptionalLong.of( transactionalShift + i ) );
            appendData.rewind();
        }

        LogFile logFile = logFiles.getLogFile();
        var logFileInformation = logFile.getLogFileInformation();
        for ( int version = (int) logVersionBefore + 1; version < logVersionBefore + appendIterations; version++ )
        {
            assertEquals( transactionalShift + version, logFileInformation.getFirstEntryId( version ) );
        }
    }

    @Test
    void replayTransactionAfterBulkAppendOnNextRestart() throws IOException
    {
        // so we will write data to system db and will mimic catchup by transfer in bulk logs from system db to test db
        var systemDatabase = (GraphDatabaseAPI) managementService.database( SYSTEM_DATABASE_NAME );
        var systemMetadata = systemDatabase.getDependencyResolver().resolveDependency( MetadataProvider.class );
        var positionBeforeTransaction = systemMetadata.getLastClosedTransaction().getLogPosition();
        for ( int i = 0; i < 3; i++ )
        {
            try ( var transaction = systemDatabase.beginTx() )
            {
                transaction.createNode();
                transaction.commit();
            }
        }
        var positionAfterTransaction = systemMetadata.getLastClosedTransaction().getLogPosition();
        long systemLastClosedTransactionId = systemMetadata.getLastClosedTransactionId();
        var buffer = readTransactionIntoBuffer( systemDatabase, positionBeforeTransaction, positionAfterTransaction );

        availabilityGuard.require( new DescriptiveAvailabilityRequirement( "Database unavailable" ) );
        long lastTransactionBeforeBufferAppend = metadataProvider.getLastClosedTransaction().getTransactionId();
        var positionBeforeRecovery = metadataProvider.getLastClosedTransaction().getLogPosition();

        for ( int i = 0; i < 3; i++ )
        {
            logService.append( buffer, OptionalLong.of( lastTransactionBeforeBufferAppend + i + 1 ) );
            buffer.rewind();
        }

        // restart db and trigger shutdown checkpoint and recovery
        Database database = databaseAPI.getDependencyResolver().resolveDependency( Database.class );
        database.stop();
        database.start();

        var restartedProvider = database.getDependencyResolver().resolveDependency( MetadataProvider.class );
        assertEquals( systemLastClosedTransactionId, restartedProvider.getLastClosedTransactionId() );
        assertNotEquals( positionBeforeRecovery, restartedProvider.getLastClosedTransaction().getLogPosition() );
    }

    @Test
    void bulkAppendRotatedLogFilesMonitorEvents() throws IOException
    {
        availabilityGuard.require( new DescriptiveAvailabilityRequirement( "Database unavailable" ) );

        BulkAppendLogRotationMonitor monitorListener = new BulkAppendLogRotationMonitor();
        databaseAPI.getDependencyResolver().resolveDependency( Monitors.class ).addMonitorListener( monitorListener );

        var appendData = createBuffer().put( randomAscii( (int) (THRESHOLD + 1) ).getBytes( UTF_8 ) );
        int appendIterations = 100;
        int transactionalShift = 10;
        for ( int i = 0; i < appendIterations; i++ )
        {
            logService.append( appendData, OptionalLong.of( transactionalShift + i ) );
            appendData.rewind();
        }

        List<Long> observedVersions = monitorListener.getObservedVersions();
        assertThat( observedVersions ).hasSize( 99 ).containsExactlyElementsOf( LongStream.range( 0, 99 ).boxed().collect( Collectors.toList() ) );
    }

    @Test
    void bulkAppendRotatedLogFilesTracingEvents() throws IOException
    {
        availabilityGuard.require( new DescriptiveAvailabilityRequirement( "Database unavailable" ) );

        DatabaseTracers databaseTracers = databaseAPI.getDependencyResolver().resolveDependency( DatabaseTracers.class );
        assertEquals( 0, databaseTracers.getDatabaseTracer().numberOfLogRotations() );

        var appendData = createBuffer().put( randomAscii( (int) (THRESHOLD + 1) ).getBytes( UTF_8 ) );
        int appendIterations = 100;
        int transactionalShift = 10;
        for ( int i = 0; i < appendIterations; i++ )
        {
            logService.append( appendData, OptionalLong.of( transactionalShift + i ) );
            appendData.rewind();
        }

        // first append is not rotated
        var expectedRotations = appendIterations - 1;
        assertEquals( expectedRotations, databaseTracers.getDatabaseTracer().numberOfLogRotations() );
    }

    @Test
    void restoreRequireNonAvailableDatabase()
    {
        assertThrows( IllegalStateException.class, () -> logService.restore( LogPosition.UNSPECIFIED ) );
    }

    @Test
    void failToRestoreWithLogPositionInHigherLogFile()
    {
        availabilityGuard.require( new DescriptiveAvailabilityRequirement( "Database unavailable" ) );

        assertThatThrownBy( () -> logService.restore( new LogPosition( metadataProvider.getCurrentLogVersion() + 5, 100 ) ) )
                .isInstanceOf( IllegalArgumentException.class )
                .hasMessage( "Log position requested for restore points to the log file that is higher than existing available highest log file. " +
                        "Requested restore position: LogPosition{logVersion=5, byteOffset=100}, current log file version: 0." );
    }

    @Test
    void failToRestoreWithLogPositionInCommittedFile()
    {
        availabilityGuard.require( new DescriptiveAvailabilityRequirement( "Database unavailable" ) );

        assertThatThrownBy( () -> logService.restore( new LogPosition( metadataProvider.getCurrentLogVersion() - 1, 100 ) ) )
                .isInstanceOf( IllegalArgumentException.class ).hasMessage(
                        "Log position requested to be used for restore belongs to the log file that was already appended by " +
                        "transaction and cannot be restored. " +
                        "Last closed position: LogPosition{logVersion=0, byteOffset=3398}, requested restore: LogPosition{logVersion=-1, byteOffset=100}" );
    }

    @Test
    void restoreOnCurrentLogVersion() throws IOException
    {
        availabilityGuard.require( new DescriptiveAvailabilityRequirement( "Database unavailable" ) );
        long logVersionBefore = metadataProvider.getCurrentLogVersion();

        var appendData = createBuffer().put( randomAscii( (int) (THRESHOLD + 1) ).getBytes( UTF_8 ) );
        int appendIterations = 100;
        LogPosition previousPosition = null;
        for ( int i = 0; i < appendIterations; i++ )
        {
            var position = logService.append( appendData, OptionalLong.empty() );
            if ( previousPosition != null )
            {
                assertEquals( previousPosition, position );
            }
            logService.restore( position );
            previousPosition = position;
            appendData.rewind();
        }

        assertEquals( logVersionBefore, logFiles.getLogFile().getHighestLogVersion() );
    }

    @Test
    void restoreInitialLogVersionAndAppend() throws IOException
    {
        availabilityGuard.require( new DescriptiveAvailabilityRequirement( "Database unavailable" ) );
        long logVersionBefore = metadataProvider.getCurrentLogVersion();

        var appendData = createBuffer().put( randomAscii( (int) (THRESHOLD + 1) ).getBytes( UTF_8 ) );
        int appendIterations = 100;
        LogPosition firstPosition = null;
        for ( int i = 0; i < appendIterations; i++ )
        {
            var position = logService.append( appendData, OptionalLong.of( i + 5 ) );
            if ( firstPosition == null )
            {
                firstPosition = position;
            }
            appendData.rewind();
        }
        assertThat( logFiles.getLogFile().getHighestLogVersion() ).isGreaterThanOrEqualTo( firstPosition.getLogVersion() );
        logService.restore( firstPosition );

        assertEquals( firstPosition, logService.append( appendData, OptionalLong.of( 5 ) ) );
        assertEquals( logVersionBefore, logFiles.getLogFile().getHighestLogVersion() );
    }

    private void createNodeInIsolatedTransaction( String propertyValue )
    {
        try ( var tx = databaseAPI.beginTx() )
        {
            Node node = tx.createNode();
            node.setProperty( "a", propertyValue );
            tx.commit();
        }
    }

    private ByteBuffer readTransactionIntoBuffer( GraphDatabaseAPI db, LogPosition positionBeforeTransaction, LogPosition positionAfterTransaction )
            throws IOException
    {
        int length = (int) (positionAfterTransaction.getByteOffset() - positionBeforeTransaction.getByteOffset());
        var data = new byte[length];
        LogFiles systemLogFiles = db.getDependencyResolver().resolveDependency( LogFiles.class );
        try ( ReadableLogChannel reader = systemLogFiles.getLogFile().getReader( positionBeforeTransaction ) )
        {
            reader.get( data, length );
        }
        return createBuffer( length ).put( data );
    }
    private long getTxStartOffset( long txId ) throws IOException
    {
        return transactionStore.getTransactions( txId ).position().getByteOffset();
    }

    private long getTxEndOffset( long txId ) throws IOException
    {
        var commandBatches = transactionStore.getTransactions( txId );
        commandBatches.next();
        return commandBatches.position().getByteOffset();
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

    private static ByteBuffer createBuffer( int length )
    {
        return ByteBuffers.allocateDirect( length, INSTANCE );
    }

    private static ByteBuffer createBuffer()
    {
        return ByteBuffers.allocateDirect( (int) (THRESHOLD << 1), INSTANCE );
    }

    private static class BulkAppendLogRotationMonitor extends LogRotationMonitorAdapter
    {
        private final List<Long> versions = new CopyOnWriteArrayList<>();
        @Override
        public void finishLogRotation( Path logFile, long logVersion, long lastTransactionId, long rotationMillis, long millisSinceLastRotation )
        {
            versions.add( logVersion );
        }

        public List<Long> getObservedVersions()
        {
            return versions;
        }
    }

    static class InjectableBeforeApplyTracers extends Tracers
    {
        InjectableBeforeApplyTracers()
        {
            super( "null", NullLog.getInstance(), new Monitors(), null, Clocks.nanoClock(), null );
        }

        @Override
        public PageCacheTracer getPageCacheTracer()
        {
            return PageCacheTracer.NULL;
        }

        @Override
        public LockTracer getLockTracer()
        {
            return LockTracer.NONE;
        }

        @Override
        public DatabaseTracer getDatabaseTracer()
        {
            return new InjectableBeforeApplyDatabaseTracer();
        }

        private static class InjectableBeforeApplyDatabaseTracer extends DefaultTracer
        {
            InjectableBeforeApplyDatabaseTracer()
            {
                super( );
            }

            @Override
            public TransactionEvent beginTransaction( CursorContext cursorContext )
            {
                return new InjectableBeforeApplyTransactionEvent( );
            }
        }

        private static class InjectableBeforeApplyTxWriteEvent implements CommitEvent
        {

            public static final InjectableBeforeApplyTxWriteEvent INSTANCE = new InjectableBeforeApplyTxWriteEvent();

            private final AtomicReference<Runnable> beforeStoreApply = new AtomicReference<>();

            @Override
            public void close()
            {
            }

            @Override
            public LogAppendEvent beginLogAppend()
            {
                return LogAppendEvent.NULL;
            }

            @Override
            public StoreApplyEvent beginStoreApply()
            {
                var beforeStoreApplyRunnable = beforeStoreApply.getAndSet( null );
                if ( beforeStoreApplyRunnable != null )
                {
                    beforeStoreApplyRunnable.run();
                }
                return StoreApplyEvent.NULL;
            }
        }

        private static class InjectableBeforeApplyTransactionEvent implements TransactionEvent
        {

            @Override
            public void setSuccess( boolean success )
            {
            }

            @Override
            public void setFailure( boolean failure )
            {
            }

            @Override
            public CommitEvent beginCommitEvent()
            {
                return InjectableBeforeApplyTxWriteEvent.INSTANCE;
            }

            @Override
            public void close()
            {
            }

            @Override
            public void setTransactionWriteState( String transactionWriteState )
            {
            }

            @Override
            public void setReadOnly( boolean wasReadOnly )
            {
            }
        }
    }
}
