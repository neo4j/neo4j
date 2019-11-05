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
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.api.TestCommand;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotationImpl;
import org.neo4j.kernel.impl.transaction.log.rotation.monitor.LogRotationMonitorAdapter;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLog;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.DatabasePanicEventGenerator;
import org.neo4j.monitoring.Health;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.Race;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.extension.Neo4jLayoutExtension;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.concurrent.locks.LockSupport.parkNanos;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.neo4j.kernel.impl.transaction.log.TestLogEntryReader.logEntryReader;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_FORMAT_LOG_HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent.NULL;

@Neo4jLayoutExtension
@ExtendWith( LifeExtension.class )
class TransactionLogAppendAndRotateIT
{
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private LifeSupport life;
    @Inject
    private DatabaseLayout databaseLayout;

    @Test
    void shouldKeepTransactionsIntactWhenConcurrentlyRotationAndAppending() throws Throwable
    {
        // GIVEN
        LogVersionRepository logVersionRepository = new SimpleLogVersionRepository();
        LogFiles logFiles = LogFilesBuilder.builder( databaseLayout, fileSystem )
                .withLogVersionRepository( logVersionRepository )
                .withRotationThreshold( ByteUnit.mebiBytes( 1 ) )
                .withTransactionIdStore( new SimpleTransactionIdStore() )
                .withLogEntryReader( logEntryReader() )
                .withStoreId( StoreId.UNKNOWN )
                .build();
        life.add( logFiles );
        final AtomicBoolean end = new AtomicBoolean();
        AllTheMonitoring monitoring = new AllTheMonitoring( end, 100 );
        TransactionIdStore txIdStore = new SimpleTransactionIdStore();
        TransactionMetadataCache metadataCache = new TransactionMetadataCache();
        monitoring.setLogFile( logFiles.getLogFile() );
        Health health = new DatabaseHealth( mock( DatabasePanicEventGenerator.class ), NullLog.getInstance() );
        LogRotation rotation = new LogRotationImpl( logFiles, Clock.systemUTC(), health, monitoring );
        final TransactionAppender appender =
                life.add( new BatchingTransactionAppender( logFiles, rotation, metadataCache, txIdStore, health ) );

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
                        fail( e.getMessage(), e );
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
        try ( ReadableLogChannel reader = logFile.getReader( new LogPosition( logVersion, CURRENT_FORMAT_LOG_HEADER_SIZE ) ) )
        {
            LogEntryReader entryReader = logEntryReader();
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
            commands.add( new TestCommand( 30 ) );
            commands.add( new TestCommand( 60 ) );
        }
        PhysicalTransactionRepresentation tx = new PhysicalTransactionRepresentation( commands );
        tx.setHeader( new byte[0], 0, 0, 0, 0 );
        return tx;
    }

    private static class AllTheMonitoring extends LogRotationMonitorAdapter
    {
        private final AtomicBoolean end;
        private final int maxNumberOfRotations;
        private final AtomicInteger rotations = new AtomicInteger();

        private volatile LogFile logFile;

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
        public void finishLogRotation( File logFile, long logVersion, long lastTransactionId, long rotationMillis, long millisSinceLastRotation )
        {
            try
            {
                assertWholeTransactionsIn( this.logFile, logVersion );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
            finally
            {
                if ( rotations.getAndIncrement() > maxNumberOfRotations )
                {
                    end.set( true );
                }
            }
        }

        int numberOfRotations()
        {
            return rotations.get();
        }
    }
}
