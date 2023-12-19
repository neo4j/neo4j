/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
