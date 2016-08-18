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
package org.neo4j.coreedge.catchup;

import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import org.neo4j.coreedge.catchup.CatchupClientProtocol.State;
import org.neo4j.coreedge.catchup.storecopy.FileContentDecoder;
import org.neo4j.coreedge.catchup.storecopy.FileHeaderDecoder;
import org.neo4j.coreedge.catchup.storecopy.GetStoreIdRequest;
import org.neo4j.coreedge.catchup.storecopy.GetStoreIdResponseDecoder;
import org.neo4j.coreedge.catchup.storecopy.GetStoreRequest;
import org.neo4j.coreedge.catchup.storecopy.StoreCopyFinishedResponseDecoder;
import org.neo4j.coreedge.catchup.storecopy.StoreFileReceiver;
import org.neo4j.coreedge.catchup.storecopy.StoreFileStreamingCompleteListener;
import org.neo4j.coreedge.catchup.storecopy.StoreFileStreams;
import org.neo4j.coreedge.catchup.storecopy.StoreIdReceiver;
import org.neo4j.coreedge.catchup.tx.PullRequestMonitor;
import org.neo4j.coreedge.catchup.tx.TxPullRequest;
import org.neo4j.coreedge.catchup.tx.TxPullResponse;
import org.neo4j.coreedge.catchup.tx.TxPullResponseDecoder;
import org.neo4j.coreedge.catchup.tx.TxPullResponseListener;
import org.neo4j.coreedge.catchup.tx.TxStreamCompleteListener;
import org.neo4j.coreedge.catchup.tx.TxStreamFinishedResponseDecoder;
import org.neo4j.coreedge.core.state.snapshot.CoreSnapshot;
import org.neo4j.coreedge.core.state.snapshot.CoreSnapshotDecoder;
import org.neo4j.coreedge.core.state.snapshot.CoreSnapshotListener;
import org.neo4j.coreedge.core.state.snapshot.CoreSnapshotRequest;
import org.neo4j.coreedge.discovery.TopologyService;
import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.coreedge.identity.StoreId;
import org.neo4j.coreedge.messaging.CoreOutbound;
import org.neo4j.coreedge.messaging.Message;
import org.neo4j.coreedge.messaging.Outbound;
import org.neo4j.coreedge.messaging.SenderService;
import org.neo4j.helpers.Listeners;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;

import static java.util.Arrays.asList;

public abstract class CoreClient extends LifecycleAdapter implements StoreFileReceiver, StoreIdReceiver,
        StoreFileStreamingCompleteListener, TxStreamCompleteListener, TxPullResponseListener, CoreSnapshotListener
{
    private final LogProvider logProvider;
    private final SenderService senderService;
    private final Outbound<MemberId,Message> outbound;
    private final PullRequestMonitor pullRequestMonitor;

    private final Listeners<StoreFileStreamingCompleteListener> storeFileStreamingCompleteListeners = new Listeners<>();
    private final Listeners<TxStreamCompleteListener> txStreamCompleteListeners = new Listeners<>();
    private final Listeners<TxPullResponseListener> txPullResponseListeners = new Listeners<>();

    private StoreFileStreams storeFileStreams;
    private Consumer<StoreId> storeIdConsumer;
    private CompletableFuture<CoreSnapshot> coreSnapshotFuture;

    public CoreClient( LogProvider logProvider, ChannelInitializer<SocketChannel> channelInitializer, Monitors monitors,
            int maxQueueSize, TopologyService discoveryService, long logThresholdMillis )
    {
        this.logProvider = logProvider;
        this.senderService = new SenderService( channelInitializer, logProvider, monitors, maxQueueSize );
        this.outbound = new CoreOutbound( discoveryService, senderService, logProvider, logThresholdMillis );
        this.pullRequestMonitor = monitors.newMonitor( PullRequestMonitor.class );
    }

    public void requestStore( MemberId serverAddress )
    {
        GetStoreRequest getStoreRequest = new GetStoreRequest();
        send( serverAddress, RequestMessageType.STORE, getStoreRequest );
    }

    public void requestStoreId( MemberId serverAddress )
    {
        GetStoreIdRequest getStoreIdRequest = new GetStoreIdRequest();
        send( serverAddress, RequestMessageType.STORE_ID, getStoreIdRequest );
    }

    public CompletableFuture<CoreSnapshot> requestCoreSnapshot( MemberId serverAddress )
    {
        coreSnapshotFuture = new CompletableFuture<>();
        CoreSnapshotRequest coreSnapshotRequest = new CoreSnapshotRequest();
        send( serverAddress, RequestMessageType.RAFT_STATE, coreSnapshotRequest );
        return coreSnapshotFuture;
    }

    public void pollForTransactions( MemberId serverAddress, StoreId storeId, long lastTransactionId )
    {
        TxPullRequest txPullRequest = new TxPullRequest( lastTransactionId, storeId );
        send( serverAddress, RequestMessageType.TX_PULL_REQUEST, txPullRequest );
        pullRequestMonitor.txPullRequest( lastTransactionId );
    }

    private void send( MemberId to, RequestMessageType messageType, Message contentMessage )
    {
        outbound.send( to, asList( messageType, contentMessage ) );
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

    public void setStoreIdConsumer( Consumer<StoreId> storeIdConsumer )
    {
        this.storeIdConsumer = storeIdConsumer;
    }

    @Override
    public void onFileStreamingComplete( long lastCommittedTxBeforeStoreCopy )
    {
        storeFileStreamingCompleteListeners.notify( listener -> listener.onFileStreamingComplete( lastCommittedTxBeforeStoreCopy ) );
    }

    @Override
    public void onTxStreamingComplete( long lastTransactionId, boolean success )
    {
        txStreamCompleteListeners.notify( listener -> listener.onTxStreamingComplete( lastTransactionId, success ) );
    }

    @Override
    public void onTxReceived( final TxPullResponse tx )
    {
        txPullResponseListeners.notify( listener -> listener.onTxReceived( tx ) );
    }

    @Override
    public void onStoreIdReceived( final StoreId storeId )
    {
        storeIdConsumer.accept( storeId );
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

    protected ChannelInboundHandler decoders( CatchupClientProtocol protocol )
    {
        RequestDecoderDispatcher<State> decoderDispatcher =
                new RequestDecoderDispatcher<>( protocol, logProvider );
        decoderDispatcher.register( State.STORE_ID, new GetStoreIdResponseDecoder() );
        decoderDispatcher.register( State.TX_PULL_RESPONSE, new TxPullResponseDecoder() );
        decoderDispatcher.register( State.CORE_SNAPSHOT, new CoreSnapshotDecoder() );
        decoderDispatcher.register( State.STORE_COPY_FINISHED, new StoreCopyFinishedResponseDecoder() );
        decoderDispatcher.register( State.TX_STREAM_FINISHED, new TxStreamFinishedResponseDecoder() );
        decoderDispatcher.register( State.FILE_HEADER, new FileHeaderDecoder() );
        decoderDispatcher.register( State.FILE_CONTENTS, new FileContentDecoder() );
        return decoderDispatcher;
    }
}
