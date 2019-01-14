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
package org.neo4j.kernel.recovery;

import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.impl.core.StartupStatisticsProvider;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.TransactionCursor;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.util.monitoring.ProgressReporter;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static org.neo4j.storageengine.api.TransactionApplicationMode.RECOVERY;
import static org.neo4j.storageengine.api.TransactionApplicationMode.REVERSE_RECOVERY;

/**
 * This is the process of doing a recovery on the transaction log and store, and is executed
 * at startup of {@link org.neo4j.kernel.NeoStoreDataSource}.
 */
public class Recovery extends LifecycleAdapter
{

    private final RecoveryService recoveryService;
    private final RecoveryMonitor monitor;
    private final StartupStatisticsProvider startupStatistics;
    private final CorruptedLogsTruncator logsTruncator;
    private final ProgressReporter progressReporter;
    private final boolean failOnCorruptedLogFiles;
    private int numberOfRecoveredTransactions;

    public Recovery( RecoveryService recoveryService, StartupStatisticsProvider startupStatistics,
            CorruptedLogsTruncator logsTruncator, RecoveryMonitor monitor, ProgressReporter progressReporter,
            boolean failOnCorruptedLogFiles )
    {
        this.recoveryService = recoveryService;
        this.monitor = monitor;
        this.startupStatistics = startupStatistics;
        this.logsTruncator = logsTruncator;
        this.progressReporter = progressReporter;
        this.failOnCorruptedLogFiles = failOnCorruptedLogFiles;
    }

    @Override
    public void init() throws IOException
    {
        RecoveryStartInformation recoveryStartInformation = recoveryService.getRecoveryStartInformation();
        if ( !recoveryStartInformation.isRecoveryRequired() )
        {
            return;
        }

        LogPosition recoveryPosition = recoveryStartInformation.getRecoveryPosition();

        monitor.recoveryRequired( recoveryPosition );
        recoveryService.startRecovery();

        LogPosition recoveryToPosition = recoveryPosition;
        CommittedTransactionRepresentation lastTransaction = null;
        CommittedTransactionRepresentation lastReversedTransaction = null;
        try
        {
            long lowestRecoveredTxId = TransactionIdStore.BASE_TX_ID;
            try ( TransactionCursor transactionsToRecover = recoveryService.getTransactionsInReverseOrder( recoveryPosition );
                    RecoveryApplier recoveryVisitor = recoveryService.getRecoveryApplier( REVERSE_RECOVERY ) )
            {
                while ( transactionsToRecover.next() )
                {
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

            try ( TransactionCursor transactionsToRecover = recoveryService.getTransactions( recoveryPosition );
                    RecoveryApplier recoveryVisitor = recoveryService.getRecoveryApplier( RECOVERY ) )
            {
                while ( transactionsToRecover.next() )
                {
                    lastTransaction = transactionsToRecover.get();
                    long txId = lastTransaction.getCommitEntry().getTxId();
                    recoveryVisitor.visit( lastTransaction );
                    monitor.transactionRecovered( txId );
                    numberOfRecoveredTransactions++;
                    recoveryToPosition = transactionsToRecover.position();
                    reportProgress();
                }
                recoveryToPosition = transactionsToRecover.position();
            }
        }
        catch ( Error | ClosedByInterruptException e )
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
                monitor.failToRecoverTransactionsAfterPosition( t, recoveryPosition );
                recoveryToPosition = recoveryPosition;
            }
        }
        progressReporter.completed();
        logsTruncator.truncate( recoveryToPosition );

        recoveryService.transactionsRecovered( lastTransaction, recoveryToPosition );
        startupStatistics.setNumberOfRecoveredTransactions( numberOfRecoveredTransactions );
        monitor.recoveryCompleted( numberOfRecoveredTransactions );
    }

    static void throwUnableToCleanRecover( Throwable t )
    {
        throw new RuntimeException(
                "Error reading transaction logs, recovery not possible. To force the database to start anyway, you can specify '" +
                        GraphDatabaseSettings.fail_on_corrupted_log_files.name() + "=false'. This will try to recover as much " +
                        "as possible and then truncate the corrupt part of the transaction log. Doing this means your database " +
                        "integrity might be compromised, please consider restoring from a consistent backup instead.", t );
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

    private long getNumberOfTransactionToRecover( RecoveryStartInformation recoveryStartInformation,
            CommittedTransactionRepresentation lastReversedTransaction )
    {
        return lastReversedTransaction.getCommitEntry().getTxId() -
                recoveryStartInformation.getFirstTxIdAfterLastCheckPoint() + 1;
    }
}
