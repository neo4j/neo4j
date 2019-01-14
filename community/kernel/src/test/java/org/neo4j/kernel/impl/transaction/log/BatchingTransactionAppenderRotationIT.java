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

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.core.DatabasePanicEventGenerator;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotationImpl;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogForceEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogForceWaitEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogRotateEvent;
import org.neo4j.kernel.impl.transaction.tracing.SerializeTransactionEvent;
import org.neo4j.kernel.impl.util.SynchronizedArrayIdOrderingQueue;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.internal.KernelEventHandlers;
import org.neo4j.kernel.lifecycle.LifeRule;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLog;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

public class BatchingTransactionAppenderRotationIT
{

    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();
    @Rule
    public final DefaultFileSystemRule fileSystem = new DefaultFileSystemRule();
    @Rule
    public final LifeRule life = new LifeRule( true );
    private final SimpleLogVersionRepository logVersionRepository = new SimpleLogVersionRepository();
    private final SimpleTransactionIdStore transactionIdStore = new SimpleTransactionIdStore();
    private final Monitors monitors = new Monitors();

    @Test
    public void correctLastAppliedToPreviousLogTransactionInHeaderOnLogFileRotation() throws IOException
    {
        LogFiles logFiles = getLogFiles( logVersionRepository, transactionIdStore );
        life.add( logFiles );
        DatabaseHealth databaseHealth = getDatabaseHealth();

        LogRotationImpl logRotation =
                new LogRotationImpl( monitors.newMonitor( LogRotation.Monitor.class ), logFiles, databaseHealth );
        TransactionMetadataCache transactionMetadataCache = new TransactionMetadataCache( 1024 );
        SynchronizedArrayIdOrderingQueue idOrderingQueue = new SynchronizedArrayIdOrderingQueue( 20 );

        BatchingTransactionAppender transactionAppender =
                new BatchingTransactionAppender( logFiles, logRotation, transactionMetadataCache, transactionIdStore,
                        idOrderingQueue, databaseHealth );

        life.add( transactionAppender );

        LogAppendEvent logAppendEvent = new RotationLogAppendEvent( logRotation );
        TransactionToApply transactionToApply = prepareTransaction();
        transactionAppender.append( transactionToApply, logAppendEvent );

        assertEquals( 1, logFiles.getHighestLogVersion() );
        File highestLogFile = logFiles.getHighestLogFile();
        LogHeader logHeader = LogHeaderReader.readLogHeader( fileSystem, highestLogFile );
        assertEquals( 2, logHeader.lastCommittedTxId );
    }

    private TransactionToApply prepareTransaction()
    {
        List<StorageCommand> commands = createCommands();
        PhysicalTransactionRepresentation transactionRepresentation = new PhysicalTransactionRepresentation( commands );
        transactionRepresentation.setHeader( new byte[0], 0, 0, 0, 0, 0, 0 );
        return new TransactionToApply( transactionRepresentation );
    }

    private List<StorageCommand> createCommands()
    {
        return singletonList( new Command.NodeCommand( new NodeRecord( 1L ), new NodeRecord( 2L ) ) );
    }

    private LogFiles getLogFiles( SimpleLogVersionRepository logVersionRepository,
            SimpleTransactionIdStore transactionIdStore ) throws IOException
    {
        return LogFilesBuilder.builder( testDirectory.directory(), fileSystem.get() )
                .withLogVersionRepository( logVersionRepository ).withTransactionIdStore( transactionIdStore ).build();
    }

    private DatabaseHealth getDatabaseHealth()
    {
        DatabasePanicEventGenerator databasePanicEventGenerator =
                new DatabasePanicEventGenerator( new KernelEventHandlers( NullLog.getInstance() ) );
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
                    logRotation.rotateLogFile();
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( "Should be able to rotate file", e );
                }
            };
        }
    }
}
