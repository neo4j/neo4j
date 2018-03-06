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
package org.neo4j.causalclustering;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.stream.ChunkedWriteHandler;

import org.neo4j.causalclustering.catchup.CatchupServerProtocol;
import org.neo4j.causalclustering.catchup.RequestDecoderDispatcher;
import org.neo4j.causalclustering.catchup.RequestMessageTypeEncoder;
import org.neo4j.causalclustering.catchup.ResponseMessageTypeEncoder;
import org.neo4j.causalclustering.catchup.ServerMessageTypeHandler;
import org.neo4j.causalclustering.catchup.storecopy.FileChunkEncoder;
import org.neo4j.causalclustering.catchup.storecopy.FileHeaderEncoder;
import org.neo4j.causalclustering.catchup.storecopy.GetIndexFilesRequest;
import org.neo4j.causalclustering.catchup.storecopy.GetStoreFileRequest;
import org.neo4j.causalclustering.catchup.storecopy.GetStoreIdResponseEncoder;
import org.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyRequestDecoder;
import org.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyResponse;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponseEncoder;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshotEncoder;
import org.neo4j.causalclustering.handlers.ExceptionLoggingHandler;
import org.neo4j.causalclustering.handlers.ExceptionSwallowingHandler;
import org.neo4j.causalclustering.handlers.PipelineWrapper;
import org.neo4j.causalclustering.handlers.VoidPipelineWrapperFactory;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.causalclustering.messaging.TestServer;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.ports.allocation.PortAuthority;

public abstract class StrippedCatchupServer
{
    private TestServer catchupServer;

    private static final LogProvider LOG_PROVIDER = NullLogProvider.getInstance();
    private int port;
    private StoreId storeId;

    public void before()
    {
        this.port = PortAuthority.allocatePort();
        this.storeId = new StoreId( 1, 2, 3, 4 );
        catchupServer = new TestServer( port, new ChannelInitializer<SocketChannel>()
        {

            @Override
            protected void initChannel( SocketChannel ch ) throws Exception
            {

                CatchupServerProtocol protocol = new CatchupServerProtocol();
                ChannelPipeline pipeline = ch.pipeline();

                PipelineWrapper pipelineWrapper = VoidPipelineWrapperFactory.VOID_WRAPPER;
                for ( ChannelHandler handler : pipelineWrapper.handlersFor( ch ) )
                {
                    pipeline.addLast( handler );
                }

                pipeline.addLast( new LengthFieldBasedFrameDecoder( Integer.MAX_VALUE, 0, 4, 0, 4 ) );
                pipeline.addLast( new LengthFieldPrepender( 4 ) );

                pipeline.addLast( new VersionDecoder( LOG_PROVIDER ) );
                pipeline.addLast( new VersionPrepender() );

                pipeline.addLast( new ResponseMessageTypeEncoder() );
                pipeline.addLast( new RequestMessageTypeEncoder() );

                pipeline.addLast( new CoreSnapshotEncoder() );
                pipeline.addLast( new GetStoreIdResponseEncoder() );
                pipeline.addLast( new StoreCopyFinishedResponseEncoder() );
                pipeline.addLast( new FileChunkEncoder() );
                pipeline.addLast( new FileHeaderEncoder() );
                pipeline.addLast( new PrepareStoreCopyResponse.Encoder() );

                pipeline.addLast( new ServerMessageTypeHandler( protocol, LOG_PROVIDER ) );

                pipeline.addLast( decoders( protocol ) );

                pipeline.addLast( new ChunkedWriteHandler() );

                pipeline.addLast( getStoreListingRequestHandler( protocol ) );
                pipeline.addLast( getStoreFileRequestHandler( protocol ) );
                pipeline.addLast( getIndexRequestHandler( protocol ) );

                pipeline.addLast( new ExceptionLoggingHandler( LOG_PROVIDER.getLog( ExceptionLoggingHandler.class ) ) );
                pipeline.addLast( new ExceptionSwallowingHandler() );
            }
        } );
    }

    public void start()
    {
        catchupServer.start();
    }

    public void stop()
    {
        catchupServer.stop();
    }

    private ChannelInboundHandler decoders( CatchupServerProtocol protocol )
    {
        RequestDecoderDispatcher<CatchupServerProtocol.State> decoderDispatcher = new RequestDecoderDispatcher<>( protocol, LOG_PROVIDER );
        decoderDispatcher.register( CatchupServerProtocol.State.PREPARE_STORE_COPY, new PrepareStoreCopyRequestDecoder() );
        decoderDispatcher.register( CatchupServerProtocol.State.GET_STORE_FILE, new GetStoreFileRequest.Decoder() );
        decoderDispatcher.register( CatchupServerProtocol.State.GET_INDEX_SNAPSHOT, new GetIndexFilesRequest.Decoder() );
        return decoderDispatcher;
    }

    protected abstract ChannelHandler getStoreFileRequestHandler( CatchupServerProtocol protocol );

    protected abstract ChannelHandler getStoreListingRequestHandler( CatchupServerProtocol protocol );

    public int getPort()
    {
        return port;
    }

    public StoreId getStoreId()
    {
        return storeId;
    }

    protected abstract ChannelHandler getIndexRequestHandler( CatchupServerProtocol protocol );
}
