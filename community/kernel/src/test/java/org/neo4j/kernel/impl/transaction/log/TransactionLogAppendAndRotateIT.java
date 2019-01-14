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
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.io.ByteUnit;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.core.DatabasePanicEventGenerator;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFileCreationMonitor;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotationImpl;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.LifeRule;
import org.neo4j.logging.NullLog;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.test.Race;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.concurrent.locks.LockSupport.parkNanos;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.rules.RuleChain.outerRule;
import static org.mockito.Mockito.mock;
import static org.neo4j.kernel.impl.transaction.command.Commands.createNode;
import static org.neo4j.kernel.impl.transaction.command.Commands.createProperty;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.LOG_HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent.NULL;
import static org.neo4j.kernel.impl.util.IdOrderingQueue.BYPASS;

public class TransactionLogAppendAndRotateIT
{
    private final LifeRule life = new LifeRule( true );
    private final TestDirectory directory = TestDirectory.testDirectory();
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Rule
    public final RuleChain chain = outerRule( directory ).around( life ).around( fileSystemRule );

    @Test
    public void shouldKeepTransactionsIntactWhenConcurrentlyRotationAndAppending() throws Throwable
    {
        // GIVEN
        LogVersionRepository logVersionRepository = new SimpleLogVersionRepository();
        LogFiles logFiles = LogFilesBuilder.builder( directory.directory(), fileSystemRule.get() )
                .withLogVersionRepository( logVersionRepository )
                .withRotationThreshold( ByteUnit.mebiBytes( 1 ) )
                .withTransactionIdStore( new SimpleTransactionIdStore() ).build();
        life.add( logFiles );
        final AtomicBoolean end = new AtomicBoolean();
        AllTheMonitoring monitoring = new AllTheMonitoring( end, 100 );
        TransactionIdStore txIdStore = new SimpleTransactionIdStore();
        TransactionMetadataCache metadataCache = new TransactionMetadataCache( 100 );
        monitoring.setLogFile( logFiles.getLogFile() );
        DatabaseHealth health = new DatabaseHealth( mock( DatabasePanicEventGenerator.class ), NullLog.getInstance() );
        LogRotation rotation = new LogRotationImpl( monitoring, logFiles, health );
        final TransactionAppender appender = life.add( new BatchingTransactionAppender( logFiles, rotation, metadataCache,
                txIdStore, BYPASS, health ) );

        // WHEN
        Race race = new Race();
        for ( int i = 0; i < 10; i++ )
        {
            race.addContestant( () ->
            {
                while ( !end.get() )
                {
                    try
                    {
                        appender.append( new TransactionToApply( sillyTransaction( 1_000 ) ), NULL );
                    }
                    catch ( Exception e )
                    {
                        e.printStackTrace( System.out );
                        end.set( true );
                        fail( e.getMessage() );
                    }
                }
            } );
        }
        race.addContestant( endAfterMax( 10, SECONDS, end ) );
        race.go();

        // THEN
        assertTrue( monitoring.numberOfRotations() > 0 );
    }

    private Runnable endAfterMax( final int time, final TimeUnit unit, final AtomicBoolean end )
    {
        return () ->
        {
            long endTime = currentTimeMillis() + unit.toMillis( time );
            while ( currentTimeMillis() < endTime && !end.get() )
            {
                parkNanos( MILLISECONDS.toNanos( 50 ) );
            }
            end.set( true );
        };
    }

    private static void assertWholeTransactionsIn( LogFile logFile, long logVersion ) throws IOException
    {
        try ( ReadableLogChannel reader = logFile.getReader( new LogPosition( logVersion, LOG_HEADER_SIZE ) ) )
        {
            VersionAwareLogEntryReader<ReadableLogChannel> entryReader = new VersionAwareLogEntryReader<>();
            LogEntry entry;
            boolean inTx = false;
            int transactions = 0;
            while ( (entry = entryReader.readLogEntry( reader )) != null )
            {
                if ( !inTx ) // Expects start entry
                {
                    assertTrue( entry instanceof LogEntryStart );
                    inTx = true;
                }
                else // Expects command/commit entry
                {
                    assertTrue( entry instanceof LogEntryCommand || entry instanceof LogEntryCommit );
                    if ( entry instanceof LogEntryCommit )
                    {
                        inTx = false;
                        transactions++;
                    }
                }
            }
            assertFalse( inTx );
            assertTrue( transactions > 0 );
        }
    }

    private TransactionRepresentation sillyTransaction( int size )
    {
        Collection<StorageCommand> commands = new ArrayList<>( size );
        for ( int i = 0; i < size; i++ )
        {
            // The actual data isn't super important
            commands.add( createNode( i ) );
            commands.add( createProperty( i, PropertyType.INT, 0 ) );
        }
        PhysicalTransactionRepresentation tx = new PhysicalTransactionRepresentation( commands );
        tx.setHeader( new byte[0], 0, 0, 0, 0, 0, 0 );
        return tx;
    }

    private static class AllTheMonitoring implements LogFileCreationMonitor, LogRotation.Monitor
    {
        private final AtomicBoolean end;
        private final int maxNumberOfRotations;

        private volatile LogFile logFile;
        private volatile int rotations;

        AllTheMonitoring( AtomicBoolean end, int maxNumberOfRotations )
        {
            this.end = end;
            this.maxNumberOfRotations = maxNumberOfRotations;
        }

        void setLogFile( LogFile logFile )
        {
            this.logFile = logFile;
        }

        @Override
        public void startedRotating( long currentVersion )
        {
        }

        @Override
        public void finishedRotating( long currentVersion )
        {
            try
            {
                assertWholeTransactionsIn( logFile, currentVersion );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
            finally
            {
                if ( rotations++ > maxNumberOfRotations )
                {
                    end.set( true );
                }
            }
        }

        int numberOfRotations()
        {
            return rotations;
        }

        @Override
        public void created( File logFile, long logVersion, long lastTransactionId )
        {
        }
    }
}
