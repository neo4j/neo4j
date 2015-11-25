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
package org.neo4j.coreedge.catchup.storecopy.edge;

import java.io.IOException;
import java.io.Serializable;

import io.netty.channel.ChannelInitializer;

import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.catchup.RequestMessageType;
import org.neo4j.coreedge.catchup.tx.edge.TxPullResponseListener;
import org.neo4j.coreedge.catchup.tx.edge.TxStreamCompleteListener;
import org.neo4j.coreedge.server.Expiration;
import org.neo4j.coreedge.server.SenderService;
import org.neo4j.coreedge.catchup.tx.edge.TxPullRequest;
import org.neo4j.coreedge.catchup.tx.edge.TxPullResponse;
import org.neo4j.coreedge.server.ExpiryScheduler;
import org.neo4j.helpers.Listeners;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.LogProvider;

public abstract class CoreClient extends LifecycleAdapter implements StoreFileReceiver, StoreFileStreamingCompleteListener,
                                                                     TxStreamCompleteListener, TxPullResponseListener
{
    private final LogProvider logProvider;
    private StoreFileStreams storeFileStreams = null;
    private Iterable<StoreFileStreamingCompleteListener> storeFileStreamingCompleteListeners = Listeners.newListeners();
    private Iterable<TxStreamCompleteListener> txStreamCompleteListeners = Listeners.newListeners();
    private Iterable<TxPullResponseListener> txPullResponseListeners = Listeners.newListeners();

    private SenderService senderService;

    public CoreClient( LogProvider logProvider, ExpiryScheduler expiryScheduler, Expiration expiration, ChannelInitializer channelInitializer )
    {
        this.logProvider = logProvider;
        this.senderService = new SenderService( expiryScheduler, expiration, channelInitializer, logProvider );
    }

    public void requestStore( AdvertisedSocketAddress from )
    {
        GetStoreRequest getStoreRequest = new GetStoreRequest();
        send( from, RequestMessageType.STORE, getStoreRequest );
    }

    public void pollForTransactions( AdvertisedSocketAddress from, long lastTransactionId )
    {
        TxPullRequest txPullRequest = new TxPullRequest( lastTransactionId );
        send( from, RequestMessageType.TX_PULL_REQUEST, txPullRequest );
    }

    protected void send( AdvertisedSocketAddress to, RequestMessageType messageType, Serializable contentMessage )
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



    public void addTxStreamCompleteListener( TxStreamCompleteListener listener )
    {
        txStreamCompleteListeners = Listeners.addListener( listener, txStreamCompleteListeners );
    }

    public void removeTxStreamCompleteListener( TxStreamCompleteListener listener )
    {
        txStreamCompleteListeners = Listeners.removeListener( listener, txStreamCompleteListeners );
    }

}
