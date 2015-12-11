/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.coreedge.catchup.tx.edge;

import java.util.function.Supplier;

import org.neo4j.coreedge.catchup.storecopy.edge.CoreClient;
import org.neo4j.coreedge.discovery.EdgeDiscoveryService;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.helpers.collection.IteratorUtil.first;
import static org.neo4j.kernel.impl.util.JobScheduler.Groups.pullUpdates;

public class TxPollingClient extends LifecycleAdapter
{
    private final JobScheduler jobScheduler;
    private final Supplier<TransactionIdStore> transactionIdStoreSupplier;
    private final EdgeDiscoveryService edgeDiscoveryService;
    private final long pollingInterval;
    private final CoreClient coreClient;
    private final TxPullResponseListener txPullResponseListener;

    public TxPollingClient( JobScheduler jobScheduler, long pollingInterval,
                            Supplier<TransactionIdStore> transactionIdStoreSupplier,
                            CoreClient coreClient, TxPullResponseListener txPullResponseListener,
                            EdgeDiscoveryService edgeDiscoveryService )
    {
        this.coreClient = coreClient;
        this.txPullResponseListener = txPullResponseListener;

        this.jobScheduler = jobScheduler;
        this.pollingInterval = pollingInterval;

        this.transactionIdStoreSupplier = transactionIdStoreSupplier;
        this.edgeDiscoveryService = edgeDiscoveryService;
    }

    public void startPolling()
    {
        coreClient.addTxPullResponseListener( txPullResponseListener );
        final TransactionIdStore transactionIdStore = transactionIdStoreSupplier.get();
        jobScheduler.scheduleRecurring( pullUpdates,
                () -> coreClient.pollForTransactions(
                        first( edgeDiscoveryService.currentTopology().getMembers() ).getCoreAddress(),
                        transactionIdStore.getLastCommittedTransactionId() ), pollingInterval, MILLISECONDS );
    }

    @Override
    public void stop() throws Throwable
    {
        coreClient.removeTxPullResponseListener( txPullResponseListener );
        jobScheduler.shutdown();
    }
}
