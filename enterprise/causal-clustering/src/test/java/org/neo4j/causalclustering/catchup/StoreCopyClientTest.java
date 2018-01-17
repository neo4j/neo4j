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
package org.neo4j.causalclustering.catchup;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.DefaultEventLoop;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.time.Clock;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.causalclustering.VersionDecoder;
import org.neo4j.causalclustering.VersionPrepender;
import org.neo4j.causalclustering.catchup.storecopy.GetStoreRequest;
import org.neo4j.causalclustering.catchup.storecopy.GetStoreRequestDecoder;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyClient;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponseEncoder;
import org.neo4j.causalclustering.common.EventLoopContext;
import org.neo4j.causalclustering.handlers.NoOpPipelineHandlerAppender;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.function.Predicates;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class StoreCopyClientTest
{
    private final CatchUpClient<EmbeddedChannel> catchUpClient =
            new CatchUpClient<>( NullLogProvider.getInstance(), Clock.systemUTC(), 2000, new Monitors(), new NoOpPipelineHandlerAppender( null, null ) );
    private EmbeddedEventLoopGroup embeddedEventLoopGroup;
    private StoreCopyClient storeCopyClient;
    private CatchupServerProtocol catchupServerProtocol = new CatchupServerProtocol();

    @Before
    public void setup()
    {
        embeddedEventLoopGroup = new EmbeddedEventLoopGroup();
        catchUpClient.bootstrap( new EventLoopContext<>( embeddedEventLoopGroup, EmbeddedChannel.class ) );
        storeCopyClient = new StoreCopyClient( catchUpClient, NullLogProvider.getInstance() );
    }

    @After
    public void after()
    {
        embeddedEventLoopGroup.embeddedChannel.close();
    }

    @Test
    public void shouldGiveLastTxId() throws Exception
    {
        RequestDecoderDispatcher<CatchupServerProtocol.State> decoderDispatcher =
                new RequestDecoderDispatcher<>( catchupServerProtocol, NullLogProvider.getInstance() );
        decoderDispatcher.register( CatchupServerProtocol.State.GET_STORE, new GetStoreRequestDecoder() );
        Interpreter clientOutputInterpreter =
                new Interpreter( new ServerMessageTypeHandler( catchupServerProtocol, NullLogProvider.getInstance() ), decoderDispatcher );
        Interpreter clientInputInterpreter = new Interpreter( new ResponseMessageTypeEncoder(), new StoreCopyFinishedResponseEncoder() );
        Object[] encodedResponse = clientInputInterpreter.encode( ResponseMessageType.STORE_COPY_FINISHED,
                new StoreCopyFinishedResponse( StoreCopyFinishedResponse.Status.SUCCESS, 10 ) );

        final AtomicReference<StoreCopyFailedException> exception = new AtomicReference<>();
        final AtomicLong txId = new AtomicLong();
        final StoreId expectedStoreId = new StoreId( 1, 2, 3, 4 );
        Thread blockingRequestThread = new Thread( () ->
        {
            try
            {
                long lastTx = storeCopyClient.copyStoreFiles( new AdvertisedSocketAddress( "something", 1234 ), expectedStoreId, null );
                txId.set( lastTx );
            }
            catch ( StoreCopyFailedException e )
            {
                exception.set( e );
            }
        } );

        // when
        blockingRequestThread.start();
        waitForCreation( embeddedEventLoopGroup );

        // when
        embeddedEventLoopGroup.embeddedChannel().writeInbound( encodedResponse );
        blockingRequestThread.join();

        // then
        assertNull( exception.get() );
        assertEquals( 10, txId.get() );

        // then - verify client output
        Object[] decode = clientOutputInterpreter.decode( embeddedEventLoopGroup.embeddedChannel() );
        assertEquals( 1, decode.length );
        GetStoreRequest givenGetStoreRequest = (GetStoreRequest) decode[0];
        assertEquals( RequestMessageType.STORE, givenGetStoreRequest.messageType() );
        assertEquals( expectedStoreId, givenGetStoreRequest.expectedStoreId() );
    }

    @Test
    @Ignore( "we currently do not handle response status in store copy client" )
    public void shouldGiveMissMatch() throws Exception
    {
        RequestDecoderDispatcher<CatchupServerProtocol.State> decoderDispatcher =
                new RequestDecoderDispatcher<>( catchupServerProtocol, NullLogProvider.getInstance() );
        decoderDispatcher.register( CatchupServerProtocol.State.GET_STORE, new GetStoreRequestDecoder() );
        Interpreter clientOutputInterpreter =
                new Interpreter( new ServerMessageTypeHandler( catchupServerProtocol, NullLogProvider.getInstance() ), decoderDispatcher );
        Interpreter clientInputInterpreter = new Interpreter( new ResponseMessageTypeEncoder(), new StoreCopyFinishedResponseEncoder() );
        Object[] encodedResponse = clientInputInterpreter.encode( ResponseMessageType.STORE_COPY_FINISHED,
                new StoreCopyFinishedResponse( StoreCopyFinishedResponse.Status.E_STORE_ID_MISMATCH, 0 ) );

        final AtomicReference<StoreCopyFailedException> exception = new AtomicReference<>();
        final StoreId expectedStoreId = new StoreId( 1, 2, 3, 4 );
        Thread blockingRequestThread = new Thread( () ->
        {
            try
            {
                storeCopyClient.copyStoreFiles( new AdvertisedSocketAddress( "something", 1234 ), expectedStoreId, null );
            }
            catch ( StoreCopyFailedException e )
            {
                exception.set( e );
            }
        } );

        // when
        blockingRequestThread.start();
        waitForCreation( embeddedEventLoopGroup );

        // when
        embeddedEventLoopGroup.embeddedChannel().writeInbound( encodedResponse );
        blockingRequestThread.join();

        // then
        assertNotNull( exception.get() );
        assertEquals( StoreCopyFailedException.class, exception.get().getClass() );

        // then - and verify client output
        Object[] decode = clientOutputInterpreter.decode( embeddedEventLoopGroup.embeddedChannel() );
        assertEquals( 1, decode.length );
        GetStoreRequest givenGetStoreRequest = (GetStoreRequest) decode[0];
        assertEquals( RequestMessageType.STORE, givenGetStoreRequest.messageType() );
        assertEquals( expectedStoreId, givenGetStoreRequest.expectedStoreId() );
    }

    class EmbeddedEventLoopGroup extends DefaultEventLoop
    {
        private volatile EmbeddedChannel embeddedChannel;

        @Override
        public ChannelFuture register( Channel channel )
        {
            embeddedChannel = (EmbeddedChannel) channel;
            return new DefaultChannelPromise( embeddedChannel, embeddedChannel.eventLoop() ).setSuccess();
        }

        public EmbeddedChannel embeddedChannel()
        {
            return embeddedChannel;
        }
    }

    class Interpreter
    {
        private final EmbeddedChannel interperator;

        Interpreter( ChannelHandler... channelHandler )
        {
            interperator = createEncoder( channelHandler );
        }

        private EmbeddedChannel createEncoder( ChannelHandler... handlers )
        {
            EmbeddedChannel embeddedChannel =
                    new EmbeddedChannel( new LengthFieldBasedFrameDecoder( Integer.MAX_VALUE, 0, 4, 0, 4 ), new LengthFieldPrepender( 4 ),
                            new VersionDecoder( NullLogProvider.getInstance() ), new VersionPrepender() );
            embeddedChannel.pipeline().addLast( handlers );
            return embeddedChannel;
        }

        Object[] encode( Object... objects )
        {
            if ( !interperator.writeOutbound( objects ) )
            {
                throw new IllegalStateException( "Failed to write to outbound" );
            }
            Object[] encodedObjects = interperator.outboundMessages().toArray();
            interperator.flushOutbound();
            return encodedObjects;
        }

        Object[] decode( EmbeddedChannel embeddedChannel )
        {
            interperator.writeInbound( embeddedChannel.outboundMessages().toArray() );
            embeddedChannel.flush();
            Object[] objects = interperator.inboundMessages().toArray();
            interperator.flushInbound();
            return objects;
        }
    }

    private void waitForCreation( EmbeddedEventLoopGroup group ) throws TimeoutException
    {
        Predicates.await( () -> group.embeddedChannel != null, 200, TimeUnit.MILLISECONDS );
    }
}
