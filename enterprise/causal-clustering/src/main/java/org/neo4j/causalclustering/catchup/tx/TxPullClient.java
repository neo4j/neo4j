/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.catchup.tx;

import java.util.concurrent.CompletableFuture;

import org.neo4j.causalclustering.catchup.CatchUpClient;
import org.neo4j.causalclustering.catchup.CatchUpClientException;
import org.neo4j.causalclustering.catchup.CatchUpResponseAdaptor;
import org.neo4j.causalclustering.catchup.TxPullRequestResult;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.monitoring.Monitors;

public class TxPullClient
{
    private final CatchUpClient catchUpClient;
    private PullRequestMonitor pullRequestMonitor;

    public TxPullClient( CatchUpClient catchUpClient, Monitors monitors )
    {
        this.catchUpClient = catchUpClient;
        this.pullRequestMonitor = monitors.newMonitor( PullRequestMonitor.class );
    }

    public TxPullRequestResult pullTransactions( AdvertisedSocketAddress fromAddress, StoreId storeId, long previousTxId,
                                                 TxPullResponseListener txPullResponseListener )
            throws CatchUpClientException
    {
        pullRequestMonitor.txPullRequest( previousTxId );
        return catchUpClient.makeBlockingRequest( fromAddress, new TxPullRequest( previousTxId, storeId ), new CatchUpResponseAdaptor<TxPullRequestResult>()
        {
            private long lastTxIdReceived = previousTxId;

            @Override
            public void onTxPullResponse( CompletableFuture<TxPullRequestResult> signal, TxPullResponse response )
            {
                this.lastTxIdReceived = response.tx().getCommitEntry().getTxId();
                txPullResponseListener.onTxReceived( response );
            }

            @Override
            public void onTxStreamFinishedResponse( CompletableFuture<TxPullRequestResult> signal, TxStreamFinishedResponse response )
            {
                signal.complete( new TxPullRequestResult( response.status(), lastTxIdReceived ) );
            }
        } );
    }
}
