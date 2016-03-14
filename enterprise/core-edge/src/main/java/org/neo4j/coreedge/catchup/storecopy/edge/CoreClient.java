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
package org.neo4j.coreedge.catchup.storecopy.edge;

import java.io.IOException;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

import org.neo4j.coreedge.catchup.RequestMessageType;
import org.neo4j.coreedge.catchup.storecopy.core.RaftStateSnapshot;
import org.neo4j.coreedge.catchup.tx.edge.PullRequestMonitor;
import org.neo4j.coreedge.catchup.tx.edge.RaftStateSnapshotListener;
import org.neo4j.coreedge.catchup.tx.edge.TxPullRequest;
import org.neo4j.coreedge.catchup.tx.edge.TxPullResponse;
import org.neo4j.coreedge.catchup.tx.edge.TxPullResponseListener;
import org.neo4j.coreedge.catchup.tx.edge.TxStreamCompleteListener;
import org.neo4j.coreedge.network.Message;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.Expiration;
import org.neo4j.coreedge.server.ExpiryScheduler;
import org.neo4j.coreedge.server.SenderService;
import org.neo4j.helpers.Listeners;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;

public abstract class CoreClient extends LifecycleAdapter implements StoreFileReceiver,
        StoreFileStreamingCompleteListener,
        TxStreamCompleteListener, TxPullResponseListener, RaftStateSnapshotListener
{
    private final PullRequestMonitor pullRequestMonitor;
    private StoreFileStreams storeFileStreams = null;
    private Iterable<StoreFileStreamingCompleteListener> storeFileStreamingCompleteListeners = Listeners.newListeners();
    private Iterable<TxStreamCompleteListener> txStreamCompleteListeners = Listeners.newListeners();
    private Iterable<TxPullResponseListener> txPullResponseListeners = Listeners.newListeners();
    private Iterable<RaftStateSnapshotListener> raftStateSnapshotListeners = Listeners.newListeners();

    private SenderService senderService;

    public CoreClient( LogProvider logProvider, ExpiryScheduler expiryScheduler, Expiration expiration,
                       ChannelInitializer<SocketChannel> channelInitializer, Monitors monitors, int maxQueueSize )
    {
        this.senderService = new SenderService( expiryScheduler, expiration, channelInitializer, logProvider,
                monitors, maxQueueSize );
        this.pullRequestMonitor = monitors.newMonitor( PullRequestMonitor.class );
    }

    public void requestStore( AdvertisedSocketAddress serverAddress )
    {
        GetStoreRequest getStoreRequest = new GetStoreRequest();
        send( serverAddress, RequestMessageType.STORE, getStoreRequest );
    }

    public void requestRaftState( AdvertisedSocketAddress serverAddress )
    {
        GetRaftStateRequest getRaftStateRequest = new GetRaftStateRequest();
        send( serverAddress, RequestMessageType.RAFT_STATE, getRaftStateRequest );
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
        txPullResponseListeners = Listeners.addListener( listener, txPullResponseListeners );
    }

    public void removeTxPullResponseListener( TxPullResponseListener listener )
    {
        txPullResponseListeners = Listeners.removeListener( listener, txPullResponseListeners );
    }

    public void addRaftStateSnapshotListener( RaftStateSnapshotListener listener )
    {
        raftStateSnapshotListeners = Listeners.addListener( listener, raftStateSnapshotListeners );
    }

    public void removeRaftStateSnapshotListener( RaftStateSnapshotListener listener )
    {
        raftStateSnapshotListeners = Listeners.removeListener( listener, raftStateSnapshotListeners );
    }

    public void addStoreFileStreamingCompleteListener( StoreFileStreamingCompleteListener listener )
    {
        storeFileStreamingCompleteListeners = Listeners.addListener( listener, storeFileStreamingCompleteListeners );
    }

    public void removeStoreFileStreamingCompleteListener( StoreFileStreamingCompleteListener listener )
    {
        storeFileStreamingCompleteListeners = Listeners.removeListener( listener, storeFileStreamingCompleteListeners );
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
    public void onFileStreamingComplete( final long lastCommittedTxBeforeStoreCopy )
    {
        Listeners.notifyListeners( storeFileStreamingCompleteListeners,
                listener -> listener.onFileStreamingComplete( lastCommittedTxBeforeStoreCopy ) );
    }

    @Override
    public void onTxStreamingComplete( final long lastTransactionId )
    {
        Listeners.notifyListeners( txStreamCompleteListeners,
                listener -> listener.onTxStreamingComplete( lastTransactionId ) );
    }

    @Override
    public void onTxReceived( final TxPullResponse tx ) throws IOException
    {
        Listeners.notifyListeners( txPullResponseListeners,
                listener -> {
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
    public void onSnapshotReceived( RaftStateSnapshot snapshot )
    {
        Listeners.notifyListeners( raftStateSnapshotListeners, listener -> listener.onSnapshotReceived( snapshot ) );
    }

    public void addTxStreamCompleteListener( TxStreamCompleteListener listener )
    {
        txStreamCompleteListeners = Listeners.addListener( listener, txStreamCompleteListeners );
    }

    public void removeTxStreamCompleteListener( TxStreamCompleteListener listener )
    {
        txStreamCompleteListeners = Listeners.removeListener( listener, txStreamCompleteListeners );
    }
}
