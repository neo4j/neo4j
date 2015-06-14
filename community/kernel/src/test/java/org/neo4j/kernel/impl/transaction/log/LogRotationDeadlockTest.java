/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.junit.Rule;
import org.junit.Test;
import org.mockito.Matchers;

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.index.IndexImplementation;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.KernelHealth;
import org.neo4j.kernel.LogRotationImpl;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.api.TransactionRepresentationStoreApplier;
import org.neo4j.kernel.impl.api.index.IndexUpdatesValidator;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.transaction.DeadSimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.impl.util.IdOrderingQueue;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.test.Barrier;
import org.neo4j.test.OtherThreadExecutor.WorkerCommand;
import org.neo4j.test.OtherThreadRule;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.api.TransactionApplicationMode.INTERNAL;

public class LogRotationDeadlockTest
{
    /**
     * Problem was that rotating thread that went to wait for all committed
     * transactions to be closed held monitor that threads needed to grab between
     * the point where transactions were committed and they were closed.
     */
    @Test
    public void shouldNotDeadlockDuringRotation() throws Exception
    {
        // GIVEN
        // controlled log rotation that will let loose a previously halted committer
        TransactionIdStore txIdStore = new DeadSimpleTransactionIdStore();
        LogFile logFile = mock( LogFile.class );
        when( logFile.getWriter() ).thenReturn( new InMemoryLogChannel() );
        Logging logging = mock( Logging.class );
        when ( logging.getMessagesLog( Matchers.<Class>any() ) ).thenReturn( mock( StringLogger.class ) );
        final Barrier.Control inBetweenCommittedAndClosed = new Barrier.Control();
        LogRotationControl rotationControl = new LogRotationControl( txIdStore, mock( IndexingService.class ),
                mock( LabelScanStore.class ), Iterables.<IndexImplementation,IndexImplementation>iterable() )
        {
            @Override
            public void awaitAllTransactionsClosed()
            {
                inBetweenCommittedAndClosed.release();
                super.awaitAllTransactionsClosed();
            }
        };
        KernelHealth health = mock( KernelHealth.class );
        LogRotationImpl rotation = new LogRotationImpl( mock( LogRotation.Monitor.class ), logFile,
                rotationControl, health, logging );

        // controlled batching transaction appender that will halt a committer
        TransactionAppender appender = new BatchingTransactionAppender( logFile, rotation,
                new TransactionMetadataCache( 10, 10 ), txIdStore, mock( IdOrderingQueue.class ),
                health )
        {
            @Override
            protected void forceAfterAppend( LogAppendEvent logAppendEvent ) throws IOException
            {
                inBetweenCommittedAndClosed.reached();
                super.forceAfterAppend( logAppendEvent );
            }
        };

        // commit process
        LogicalTransactionStore txStore = mock( LogicalTransactionStore.class );
        when( txStore.getAppender() ).thenReturn( appender );
        TransactionCommitProcess commitProcess = new TransactionRepresentationCommitProcess( txStore,
                health, txIdStore, mock( TransactionRepresentationStoreApplier.class ),
                mock( IndexUpdatesValidator.class ) );

        // WHEN
        // trapping an appender in between having its transaction committed and closed
        Future<Void> appendFuture = committer.execute( commitArbitraryTransaction( commitProcess ) );
        inBetweenCommittedAndClosed.await();

        // and another transaction appender comes in and wants to rotate the log,
        // where the rotation resumes the first appender at the point where it starts awaiting
        // committed transactions to be closed
        when( logFile.rotationNeeded() ).thenReturn( true );
        Future<Void> rotateFuture = rotator.execute( commitArbitraryTransaction( commitProcess ) );

        // THEN
        // first appender should be able to complete once we let it loose
        appendFuture.get( 100, TimeUnit.SECONDS );
        // and its completion should let the rotation be able to complete
        rotateFuture.get( 100, TimeUnit.SECONDS );
    }

    private WorkerCommand<Void,Void> commitArbitraryTransaction( final TransactionCommitProcess commitProcess )
    {
        return new WorkerCommand<Void,Void>()
        {
            @Override
            public Void doWork( Void state ) throws Exception
            {
                commitProcess.commit( arbitraryTransaction(), new LockGroup(), CommitEvent.NULL, INTERNAL );
                return null;
            }
        };
    }

    private TransactionRepresentation arbitraryTransaction()
    {
        TransactionRepresentation transaction = mock( TransactionRepresentation.class );
        when( transaction.additionalHeader() ).thenReturn( new byte[0] );
        return transaction;
    }

    public final @Rule OtherThreadRule<Void> committer = new OtherThreadRule<>( "COMMITTER" );
    public final @Rule OtherThreadRule<Void> rotator = new OtherThreadRule<>( "ROTATOR" );
}
