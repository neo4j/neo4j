/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.catchup.tx;

import java.util.concurrent.CompletableFuture;

import org.neo4j.causalclustering.catchup.CatchUpClient;
import org.neo4j.causalclustering.catchup.CatchUpClientException;
import org.neo4j.causalclustering.catchup.CatchUpResponseAdaptor;
import org.neo4j.causalclustering.catchup.TxPullRequestResult;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.causalclustering.messaging.EventHandler;
import org.neo4j.causalclustering.messaging.EventHandlerProvider;
import org.neo4j.causalclustering.messaging.EventId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.monitoring.Monitors;

import static org.neo4j.causalclustering.messaging.EventHandler.EventState.Begin;
import static org.neo4j.causalclustering.messaging.EventHandler.EventState.End;
import static org.neo4j.causalclustering.messaging.EventHandler.Param.param;

public class TxPullClient
{
    private final CatchUpClient catchUpClient;
    private PullRequestMonitor pullRequestMonitor;
    private final EventHandlerProvider eventHandlerProvider;

    public TxPullClient( CatchUpClient catchUpClient, Monitors monitors, EventHandlerProvider eventHandlerProvider )
    {
        this.catchUpClient = catchUpClient;
        this.pullRequestMonitor = monitors.newMonitor( PullRequestMonitor.class );
        this.eventHandlerProvider = eventHandlerProvider;
    }

    public TxPullRequestResult pullTransactions( AdvertisedSocketAddress fromAddress, StoreId storeId, long previousTxId,
                                                 TxPullResponseListener txPullResponseListener )
            throws CatchUpClientException
    {
        pullRequestMonitor.txPullRequest( previousTxId );
        EventId eventId = EventId.create();
        EventHandler eventHandler = eventHandlerProvider.eventHandler( eventId );
        eventHandler.on( Begin, param( "Previous txId", previousTxId ), param( "Address", fromAddress ) );
        return catchUpClient.makeBlockingRequest( fromAddress, new TxPullRequest( previousTxId, storeId, eventId.toString() ),
                new CatchUpResponseAdaptor<TxPullRequestResult>()
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
                eventHandler.on( End, param( "Response status", response.status() ), param( "Last TxId", response.latestTxId() ) );
            }
        } );
    }
}
