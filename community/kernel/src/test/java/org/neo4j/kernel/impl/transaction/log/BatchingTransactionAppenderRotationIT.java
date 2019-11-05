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
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.time.Clock;
import java.util.List;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.api.TestCommand;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotationImpl;
import org.neo4j.kernel.impl.transaction.log.rotation.monitor.LogRotationMonitor;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogForceEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogForceWaitEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogRotateEvent;
import org.neo4j.kernel.impl.transaction.tracing.SerializeTransactionEvent;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLog;
import org.neo4j.monitoring.DatabaseEventListeners;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.DatabasePanicEventGenerator;
import org.neo4j.monitoring.Health;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.extension.Neo4jLayoutExtension;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@Neo4jLayoutExtension
@ExtendWith( LifeExtension.class )
class BatchingTransactionAppenderRotationIT
{
    @Inject
    private DatabaseLayout layout;
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private LifeSupport life;
    private final SimpleLogVersionRepository logVersionRepository = new SimpleLogVersionRepository();
    private final SimpleTransactionIdStore transactionIdStore = new SimpleTransactionIdStore();
    private final Monitors monitors = new Monitors();

    @Test
    void correctLastAppliedToPreviousLogTransactionInHeaderOnLogFileRotation() throws IOException
    {
        LogFiles logFiles = getLogFiles( logVersionRepository, transactionIdStore );
        life.add( logFiles );
        Health databaseHealth = getDatabaseHealth();

        LogRotationImpl logRotation =
                new LogRotationImpl( logFiles, Clock.systemUTC(), databaseHealth, monitors.newMonitor( LogRotationMonitor.class ) );
        TransactionMetadataCache transactionMetadataCache = new TransactionMetadataCache();

        BatchingTransactionAppender transactionAppender =
                new BatchingTransactionAppender( logFiles, logRotation, transactionMetadataCache, transactionIdStore, databaseHealth );

        life.add( transactionAppender );

        LogAppendEvent logAppendEvent = new RotationLogAppendEvent( logRotation );
        TransactionToApply transactionToApply = prepareTransaction();
        transactionAppender.append( transactionToApply, logAppendEvent );

        assertEquals( 1, logFiles.getHighestLogVersion() );
        File highestLogFile = logFiles.getHighestLogFile();
        LogHeader logHeader = LogHeaderReader.readLogHeader( fileSystem, highestLogFile );
        assertEquals( 2, logHeader.getLastCommittedTxId() );
    }

    private static TransactionToApply prepareTransaction()
    {
        List<StorageCommand> commands = createCommands();
        PhysicalTransactionRepresentation transactionRepresentation = new PhysicalTransactionRepresentation( commands );
        transactionRepresentation.setHeader( new byte[0], 0, 0, 0, 0 );
        return new TransactionToApply( transactionRepresentation );
    }

    private static List<StorageCommand> createCommands()
    {
        return singletonList( new TestCommand() );
    }

    private LogFiles getLogFiles( SimpleLogVersionRepository logVersionRepository,
            SimpleTransactionIdStore transactionIdStore ) throws IOException
    {
        return LogFilesBuilder.builder( layout, fileSystem )
                .withLogVersionRepository( logVersionRepository )
                .withTransactionIdStore( transactionIdStore )
                .withLogEntryReader( new VersionAwareLogEntryReader( new TestCommandReaderFactory() ) )
                .withStoreId( StoreId.UNKNOWN )
                .build();
    }

    private static Health getDatabaseHealth()
    {
        DatabasePanicEventGenerator databasePanicEventGenerator =
                new DatabasePanicEventGenerator( new DatabaseEventListeners( NullLog.getInstance() ), DEFAULT_DATABASE_NAME );
        return new DatabaseHealth( databasePanicEventGenerator, NullLog.getInstance() );
    }

    private static class RotationLogAppendEvent implements LogAppendEvent
    {

        private final LogRotationImpl logRotation;

        RotationLogAppendEvent( LogRotationImpl logRotation )
        {
            this.logRotation = logRotation;
        }

        @Override
        public LogForceWaitEvent beginLogForceWait()
        {
            return null;
        }

        @Override
        public LogForceEvent beginLogForce()
        {
            return null;
        }

        @Override
        public void appendToLogFile( LogPosition logPositionBeforeAppend, LogPosition logPositionAfterAppend )
        {

        }

        @Override
        public void close()
        {
        }

        @Override
        public void setLogRotated( boolean logRotated )
        {

        }

        @Override
        public LogRotateEvent beginLogRotate()
        {
            return null;
        }

        @Override
        public SerializeTransactionEvent beginSerializeTransaction()
        {
            return () ->
            {
                try
                {
                    logRotation.rotateLogFile( LogAppendEvent.NULL );
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( "Should be able to rotate file", e );
                }
            };
        }
    }
}
