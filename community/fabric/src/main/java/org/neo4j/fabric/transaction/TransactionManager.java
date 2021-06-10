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
package org.neo4j.fabric.transaction;

import java.time.Clock;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.fabric.bookmark.TransactionBookmarkManager;
import org.neo4j.fabric.config.FabricConfig;
import org.neo4j.fabric.executor.FabricLocalExecutor;
import org.neo4j.fabric.executor.FabricRemoteExecutor;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.locks.LockSupport.parkNanos;

public class TransactionManager extends LifecycleAdapter
{

    private final FabricRemoteExecutor remoteExecutor;
    private final FabricLocalExecutor localExecutor;
    private final ErrorReporter errorReporter;
    private final FabricConfig fabricConfig;
    private final FabricTransactionMonitor transactionMonitor;
    private final Clock clock;

    private final Set<FabricTransactionImpl> openTransactions = ConcurrentHashMap.newKeySet();
    private final long awaitActiveTransactionDeadlineMillis;
    private final AvailabilityGuard availabilityGuard;

    public TransactionManager( FabricRemoteExecutor remoteExecutor,
            FabricLocalExecutor localExecutor,
            ErrorReporter errorReporter,
            FabricConfig fabricConfig,
            FabricTransactionMonitor transactionMonitor,
            Clock clock, Config config, AvailabilityGuard availabilityGuard )
    {
        this.remoteExecutor = remoteExecutor;
        this.localExecutor = localExecutor;
        this.errorReporter = errorReporter;
        this.fabricConfig = fabricConfig;
        this.transactionMonitor = transactionMonitor;
        this.clock = clock;
        this.awaitActiveTransactionDeadlineMillis = config.get( GraphDatabaseSettings.shutdown_transaction_end_timeout ).toMillis();
        this.availabilityGuard = availabilityGuard;
    }

    public FabricTransaction begin( FabricTransactionInfo transactionInfo, TransactionBookmarkManager transactionBookmarkManager )
    {
        if ( availabilityGuard.isShutdown() )
        {
            throw new DatabaseShutdownException();
        }
        transactionInfo.getLoginContext().authorize( LoginContext.IdLookup.EMPTY, transactionInfo.getSessionDatabaseId().name() );

        FabricTransactionImpl fabricTransaction = new FabricTransactionImpl( transactionInfo,
                transactionBookmarkManager,
                remoteExecutor,
                localExecutor,
                errorReporter,
                this,
                fabricConfig );

        openTransactions.add( fabricTransaction );
        transactionMonitor.startMonitoringTransaction( fabricTransaction, transactionInfo );
        return fabricTransaction;
    }

    @Override
    public void stop()
    {
        // On a fabric level we will deal with transactions that a cross DBMS.
        // Any db specific transaction will be handled on a database level with own set of rules, checks etc
        var nonLocalTransaction = collectNonLocalTransactions();
        if ( nonLocalTransaction.isEmpty() )
        {
            return;
        }
        awaitTransactionsClosedWithinTimeout( nonLocalTransaction );
        nonLocalTransaction.forEach( tx -> tx.markForTermination( Status.Transaction.Terminated ) );
    }

    private Collection<FabricTransactionImpl> collectNonLocalTransactions()
    {
        return openTransactions.stream().filter( tx -> !tx.isLocal() ).collect( Collectors.toList() );
    }

    private void awaitTransactionsClosedWithinTimeout( Collection<FabricTransactionImpl> nonLocalTransaction )
    {
        long deadline = clock.millis() + awaitActiveTransactionDeadlineMillis;
        while ( hasOpenTransactions( nonLocalTransaction ) && clock.millis() < deadline )
        {
            parkNanos( MILLISECONDS.toNanos( 10 ) );
        }
    }

    private static boolean hasOpenTransactions( Collection<FabricTransactionImpl> nonLocalTransaction )
    {
        for ( FabricTransactionImpl fabricTransaction : nonLocalTransaction )
        {
            if ( fabricTransaction.isOpen() )
            {
                return true;
            }
        }
        return false;
    }

    void removeTransaction( FabricTransactionImpl transaction )
    {
        openTransactions.remove( transaction );
        transactionMonitor.stopMonitoringTransaction( transaction );
    }

    public Set<FabricTransaction> getOpenTransactions()
    {
        return Collections.unmodifiableSet( openTransactions );
    }

    public Optional<FabricTransaction> findTransactionContaining( InternalTransaction transaction )
    {
        return openTransactions.stream()
                               .filter( tx -> tx.getInternalTransactions().stream()
                                                .anyMatch( itx -> itx.kernelTransaction() == transaction.kernelTransaction() ) )
                               .map( FabricTransaction.class::cast )
                               .findFirst();
    }
}
