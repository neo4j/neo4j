/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.catchup.storecopy;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import org.neo4j.coreedge.catchup.RequestMessageType;
import org.neo4j.coreedge.catchup.storecopy.core.CoreSnapshotListener;
import org.neo4j.coreedge.catchup.storecopy.core.CoreSnapshotRequest;
import org.neo4j.coreedge.catchup.storecopy.edge.GetStoreRequest;
import org.neo4j.coreedge.catchup.storecopy.edge.StoreFileReceiver;
import org.neo4j.coreedge.catchup.storecopy.edge.StoreFileStreamingCompleteListener;
import org.neo4j.coreedge.catchup.storecopy.edge.StoreFileStreams;
import org.neo4j.coreedge.catchup.tx.edge.PullRequestMonitor;
import org.neo4j.coreedge.catchup.tx.edge.TxPullRequest;
import org.neo4j.coreedge.catchup.tx.edge.TxPullResponse;
import org.neo4j.coreedge.catchup.tx.edge.TxPullResponseListener;
import org.neo4j.coreedge.catchup.tx.edge.TxStreamCompleteListener;
import org.neo4j.coreedge.network.Message;
import org.neo4j.coreedge.raft.state.CoreSnapshot;
import org.neo4j.coreedge.server.NonBlockingChannels;
import org.neo4j.coreedge.server.SenderService;
import org.neo4j.server.AdvertisedSocketAddress;
import org.neo4j.helpers.Listeners;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;

public abstract class CoreClient extends LifecycleAdapter implements StoreFileReceiver,
                                                                     StoreFileStreamingCompleteListener,
                                                                     TxStreamCompleteListener, TxPullResponseListener, CoreSnapshotListener
{
    private final PullRequestMonitor pullRequestMonitor;
    private StoreFileStreams storeFileStreams = null;
    private final Listeners<StoreFileStreamingCompleteListener> storeFileStreamingCompleteListeners = new Listeners<>();
    private final Listeners<TxStreamCompleteListener> txStreamCompleteListeners = new Listeners<>();
    private final Listeners<TxPullResponseListener> txPullResponseListeners = new Listeners<>();
    private CompletableFuture<CoreSnapshot> coreSnapshotFuture;

    private SenderService senderService;

    public CoreClient( LogProvider logProvider,
                       ChannelInitializer<SocketChannel> channelInitializer, Monitors monitors, int maxQueueSize,
                       NonBlockingChannels nonBlockingChannels )
    {
        this.senderService = new SenderService( channelInitializer, logProvider,
                monitors, maxQueueSize, nonBlockingChannels );
        this.pullRequestMonitor = monitors.newMonitor( PullRequestMonitor.class );
    }

    public void requestStore( AdvertisedSocketAddress serverAddress )
    {
        GetStoreRequest getStoreRequest = new GetStoreRequest();
        send( serverAddress, RequestMessageType.STORE, getStoreRequest );
    }

    public CompletableFuture<CoreSnapshot> requestCoreSnapshot( AdvertisedSocketAddress serverAddress )
    {
        coreSnapshotFuture = new CompletableFuture<>();
        CoreSnapshotRequest coreSnapshotRequest = new CoreSnapshotRequest();
        send( serverAddress, RequestMessageType.RAFT_STATE, coreSnapshotRequest );
        return coreSnapshotFuture;
    }

    public void pollForTransactions( AdvertisedSocketAddress serverAddress, long lastTransactionId )
    {
        TxPullRequest txPullRequest = new TxPullRequest( lastTransactionId );
        send( serverAddress, RequestMessageType.TX_PULL_REQUEST, txPullRequest );
        pullRequestMonitor.txPullRequest( lastTransactionId );
    }

    private void send( AdvertisedSocketAddress to, RequestMessageType messageType, Message contentMessage )
    {
        senderService.send( to, messageType, contentMessage );
    }

    @Override
    public void start() throws Throwable
    {
        senderService.start();
    }

    @Override
    public void stop() throws Throwable
    {
        senderService.stop();
    }

    public void addTxPullResponseListener( TxPullResponseListener listener )
    {
        txPullResponseListeners.add( listener );
    }

    public void removeTxPullResponseListener( TxPullResponseListener listener )
    {
        txPullResponseListeners.remove( listener );
    }

    public void addStoreFileStreamingCompleteListener( StoreFileStreamingCompleteListener listener )
    {
        storeFileStreamingCompleteListeners.add( listener );
    }

    public void removeStoreFileStreamingCompleteListener( StoreFileStreamingCompleteListener listener )
    {
        storeFileStreamingCompleteListeners.remove( listener );
    }

    @Override
    public StoreFileStreams getStoreFileStreams()
    {
        return storeFileStreams;
    }

    public void setStoreFileStreams( StoreFileStreams storeFileStreams )
    {
        this.storeFileStreams = storeFileStreams;
    }

    @Override
    public void onFileStreamingComplete( long lastCommittedTxBeforeStoreCopy )
    {
        storeFileStreamingCompleteListeners.notify(
                listener -> listener.onFileStreamingComplete( lastCommittedTxBeforeStoreCopy ) );
    }

    @Override
    public void onTxStreamingComplete( long lastTransactionId )
    {
        txStreamCompleteListeners.notify( listener -> listener.onTxStreamingComplete( lastTransactionId ) );
    }

    @Override
    public void onTxReceived( TxPullResponse tx ) throws IOException
    {
        txPullResponseListeners.notify( listener -> {
            try
            {
                listener.onTxReceived( tx );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        } );
    }

    @Override
    public void onSnapshotReceived( CoreSnapshot snapshot )
    {
        coreSnapshotFuture.complete( snapshot );
    }

    public void addTxStreamCompleteListener( TxStreamCompleteListener listener )
    {
        txStreamCompleteListeners.add( listener );
    }

    public void removeTxStreamCompleteListener( TxStreamCompleteListener listener )
    {
        txStreamCompleteListeners.remove( listener );
    }
}
