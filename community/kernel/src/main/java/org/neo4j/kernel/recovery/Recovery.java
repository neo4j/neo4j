/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.recovery;

import org.neo4j.kernel.impl.core.StartupStatisticsProvider;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.TransactionCursor;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
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
    private final TransactionLogPruner logPruner;
    private int numberOfRecoveredTransactions;

    public Recovery( RecoveryService recoveryService, StartupStatisticsProvider startupStatistics,
            TransactionLogPruner logPruner, RecoveryMonitor monitor )
    {
        this.recoveryService = recoveryService;
        this.monitor = monitor;
        this.startupStatistics = startupStatistics;
        this.logPruner = logPruner;
    }

    @Override
    public void init() throws Throwable
    {
        LogPosition recoveryFromPosition = recoveryService.getPositionToRecoverFrom();
        if ( LogPosition.UNSPECIFIED.equals( recoveryFromPosition ) )
        {
            return;
        }

        monitor.recoveryRequired( recoveryFromPosition );
        recoveryService.startRecovery();

        LogPosition recoveryToPosition = recoveryFromPosition;
        CommittedTransactionRepresentation lastTransaction = null;
        CommittedTransactionRepresentation lastReversedTransaction = null;
        try
        {
            long lowestRecoveredTxId = TransactionIdStore.BASE_TX_ID;
            try ( TransactionCursor transactionsToRecover = recoveryService.getTransactionsInReverseOrder( recoveryFromPosition );
                    RecoveryApplier recoveryVisitor = recoveryService.getRecoveryApplier( REVERSE_RECOVERY ) )
            {
                while ( transactionsToRecover.next() )
                {
                    CommittedTransactionRepresentation transaction = transactionsToRecover.get();
                    if ( lastReversedTransaction == null )
                    {
                        lastReversedTransaction = transaction;
                    }
                    recoveryVisitor.visit( transaction );
                    lowestRecoveredTxId = transaction.getCommitEntry().getTxId();
                }
            }

            monitor.reverseStoreRecoveryCompleted( lowestRecoveredTxId );

            try ( TransactionCursor transactionsToRecover = recoveryService.getTransactions( recoveryFromPosition );
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
                }
            }
        }
        catch ( Exception e )
        {
            if ( lastTransaction != null )
            {
                LogEntryCommit commitEntry = lastTransaction.getCommitEntry();
                monitor.failToRecoverTransactionsAfterCommit( e, commitEntry, recoveryToPosition );
            }
            else
            {
                monitor.failToRecoverTransactionsAfterPosition( e, recoveryFromPosition );
                recoveryToPosition = recoveryFromPosition;
            }
        }
        logPruner.prune( recoveryToPosition );

        recoveryService.transactionsRecovered( lastTransaction, recoveryToPosition );
        startupStatistics.setNumberOfRecoveredTransactions( numberOfRecoveredTransactions );
        monitor.recoveryCompleted( numberOfRecoveredTransactions );
    }
}
