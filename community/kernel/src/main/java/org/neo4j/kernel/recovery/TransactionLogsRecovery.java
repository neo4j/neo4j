/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.recovery;

import java.nio.channels.ClosedByInterruptException;

import org.neo4j.common.ProgressReporter;
import org.neo4j.dbms.database.DatabaseStartAbortedException;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.TransactionCursor;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.time.Stopwatch;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.kernel.recovery.Recovery.throwUnableToCleanRecover;
import static org.neo4j.storageengine.api.TransactionApplicationMode.RECOVERY;
import static org.neo4j.storageengine.api.TransactionApplicationMode.REVERSE_RECOVERY;

/**
 * This is the process of doing a recovery on the transaction log and store, and is executed
 * at startup of {@link Database}.
 */
public class TransactionLogsRecovery extends LifecycleAdapter
{
    private static final String REVERSE_RECOVERY_TAG = "restoreDatabase";
    private static final String RECOVERY_TAG = "recoverDatabase";
    private static final String RECOVERY_COMPLETED_TAG = "databaseRecoveryCompleted";

    private final RecoveryService recoveryService;
    private final RecoveryMonitor monitor;
    private final CorruptedLogsTruncator logsTruncator;
    private final Lifecycle schemaLife;
    private final ProgressReporter progressReporter;
    private final boolean failOnCorruptedLogFiles;
    private final RecoveryStartupChecker recoveryStartupChecker;
    private final PageCacheTracer pageCacheTracer;
    private int numberOfRecoveredTransactions;

    public TransactionLogsRecovery( RecoveryService recoveryService, CorruptedLogsTruncator logsTruncator, Lifecycle schemaLife,
            RecoveryMonitor monitor, ProgressReporter progressReporter, boolean failOnCorruptedLogFiles, RecoveryStartupChecker recoveryStartupChecker,
            PageCacheTracer pageCacheTracer )
    {
        this.recoveryService = recoveryService;
        this.monitor = monitor;
        this.logsTruncator = logsTruncator;
        this.schemaLife = schemaLife;
        this.progressReporter = progressReporter;
        this.failOnCorruptedLogFiles = failOnCorruptedLogFiles;
        this.recoveryStartupChecker = recoveryStartupChecker;
        this.pageCacheTracer = pageCacheTracer;
    }

    @Override
    public void init() throws Exception
    {
        RecoveryStartInformation recoveryStartInformation = recoveryService.getRecoveryStartInformation();
        if ( !recoveryStartInformation.isRecoveryRequired() )
        {
            schemaLife.init();
            return;
        }

        Stopwatch recoveryStartTime = Stopwatch.start();

        LogPosition recoveryStartPosition = recoveryStartInformation.getRecoveryPosition();

        monitor.recoveryRequired( recoveryStartPosition );

        LogPosition recoveryToPosition = recoveryStartPosition;
        LogPosition lastTransactionPosition = recoveryStartPosition;
        CommittedTransactionRepresentation lastTransaction = null;
        CommittedTransactionRepresentation lastReversedTransaction = null;
        if ( !recoveryStartInformation.isMissingLogs() )
        {
            try
            {
                long lowestRecoveredTxId = TransactionIdStore.BASE_TX_ID;
                try ( var transactionsToRecover = recoveryService.getTransactionsInReverseOrder( recoveryStartPosition );
                      var cursorTracer = pageCacheTracer.createPageCursorTracer( REVERSE_RECOVERY_TAG );
                      var recoveryVisitor = recoveryService.getRecoveryApplier( REVERSE_RECOVERY, cursorTracer ) )
                {
                    while ( transactionsToRecover.next() )
                    {
                        recoveryStartupChecker.checkIfCanceled();
                        CommittedTransactionRepresentation transaction = transactionsToRecover.get();
                        if ( lastReversedTransaction == null )
                        {
                            lastReversedTransaction = transaction;
                            initProgressReporter( recoveryStartInformation, lastReversedTransaction );
                        }
                        recoveryVisitor.visit( transaction );
                        lowestRecoveredTxId = transaction.getCommitEntry().getTxId();
                        reportProgress();
                    }
                }

                monitor.reverseStoreRecoveryCompleted( lowestRecoveredTxId );

                // We cannot initialise the schema (tokens, schema cache, indexing service, etc.) until we have returned the store to a consistent state.
                // We need to be able to read the store before we can even figure out what indexes, tokens, etc. we have. Hence we defer the initialisation
                // of the schema life until after we've done the reverse recovery.
                schemaLife.init();

                try ( TransactionCursor transactionsToRecover = recoveryService.getTransactions( recoveryStartPosition );
                        var cursorTracer = pageCacheTracer.createPageCursorTracer( RECOVERY_TAG );
                        RecoveryApplier recoveryVisitor = recoveryService.getRecoveryApplier( RECOVERY, cursorTracer ) )
                {
                    while ( transactionsToRecover.next() )
                    {
                        recoveryStartupChecker.checkIfCanceled();
                        lastTransaction = transactionsToRecover.get();
                        long txId = lastTransaction.getCommitEntry().getTxId();
                        recoveryVisitor.visit( lastTransaction );
                        monitor.transactionRecovered( txId );
                        numberOfRecoveredTransactions++;
                        lastTransactionPosition = transactionsToRecover.position();
                        recoveryToPosition = lastTransactionPosition;
                        reportProgress();
                    }
                    recoveryToPosition = transactionsToRecover.position();
                }
            }
            catch ( Error | ClosedByInterruptException | DatabaseStartAbortedException e )
            {
                // We do not want to truncate logs based on these exceptions. Since users can influence them with config changes
                // the users are able to workaround this if truncations is really needed.
                throw e;
            }
            catch ( Throwable t )
            {
                if ( failOnCorruptedLogFiles )
                {
                    throwUnableToCleanRecover( t );
                }
                if ( lastTransaction != null )
                {
                    LogEntryCommit commitEntry = lastTransaction.getCommitEntry();
                    monitor.failToRecoverTransactionsAfterCommit( t, commitEntry, recoveryToPosition );
                }
                else
                {
                    monitor.failToRecoverTransactionsAfterPosition( t, recoveryStartPosition );
                }
            }
            progressReporter.completed();
            logsTruncator.truncate( recoveryToPosition );
        }

        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( RECOVERY_COMPLETED_TAG ) )
        {
            final boolean missingLogs = recoveryStartInformation.isMissingLogs();
            recoveryService.transactionsRecovered( lastTransaction, lastTransactionPosition, recoveryToPosition, missingLogs, cursorTracer );
        }
        monitor.recoveryCompleted( numberOfRecoveredTransactions, recoveryStartTime.elapsed( MILLISECONDS ) );
    }

    private void initProgressReporter( RecoveryStartInformation recoveryStartInformation,
            CommittedTransactionRepresentation lastReversedTransaction )
    {
        long numberOfTransactionToRecover =
                getNumberOfTransactionToRecover( recoveryStartInformation, lastReversedTransaction );
        // since we will process each transaction twice (doing reverse and direct detour) we need to
        // multiply number of transactions that we want to recover by 2 to be able to report correct progress
        progressReporter.start( numberOfTransactionToRecover * 2 );
    }

    private void reportProgress()
    {
        progressReporter.progress( 1 );
    }

    private static long getNumberOfTransactionToRecover( RecoveryStartInformation recoveryStartInformation,
            CommittedTransactionRepresentation lastReversedTransaction )
    {
        return lastReversedTransaction.getCommitEntry().getTxId() -
                recoveryStartInformation.getFirstTxIdAfterLastCheckPoint() + 1;
    }

    @Override
    public void start() throws Exception
    {
        schemaLife.start();
    }

    @Override
    public void stop() throws Exception
    {
        schemaLife.stop();
    }

    @Override
    public void shutdown() throws Exception
    {
        schemaLife.shutdown();
    }
}
