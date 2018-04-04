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
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInboundHandler;
import io.netty.handler.stream.ChunkedWriteHandler;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.catchup.storecopy.FileChunkEncoder;
import org.neo4j.causalclustering.catchup.storecopy.FileHeaderEncoder;
import org.neo4j.causalclustering.catchup.storecopy.GetIndexFilesRequest;
import org.neo4j.causalclustering.catchup.storecopy.GetStoreFileRequest;
import org.neo4j.causalclustering.catchup.storecopy.GetStoreIdRequest;
import org.neo4j.causalclustering.catchup.storecopy.GetStoreIdResponseEncoder;
import org.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyRequestDecoder;
import org.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyResponse;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponseEncoder;
import org.neo4j.causalclustering.catchup.tx.TxPullRequestDecoder;
import org.neo4j.causalclustering.catchup.tx.TxPullResponseEncoder;
import org.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponseEncoder;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshotEncoder;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshotRequest;
import org.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import org.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import org.neo4j.causalclustering.protocol.Protocol;
import org.neo4j.causalclustering.protocol.ProtocolInstaller;
import org.neo4j.causalclustering.protocol.ProtocolInstaller.Orientation;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.Collections.emptyList;

public class CatchupProtocolServerInstaller implements ProtocolInstaller<Orientation.Server>
{
    private static final Protocol.ApplicationProtocols APPLICATION_PROTOCOL = Protocol.ApplicationProtocols.CATCHUP_1;

    public static class Factory extends ProtocolInstaller.Factory<Orientation.Server,CatchupProtocolServerInstaller>
    {
        public Factory( NettyPipelineBuilderFactory pipelineBuilderFactory, LogProvider logProvider, CatchupServerHandler catchupServerHandler )
        {
            super( APPLICATION_PROTOCOL,
                    modifiers -> new CatchupProtocolServerInstaller( pipelineBuilderFactory, modifiers, logProvider, catchupServerHandler ) );
        }
    }

    private final NettyPipelineBuilderFactory pipelineBuilderFactory;
    private final List<ModifierProtocolInstaller<Orientation.Server>> modifiers;
    private final Log log;

    private final LogProvider logProvider;
    private final CatchupServerHandler catchupServerHandler;

    private CatchupProtocolServerInstaller( NettyPipelineBuilderFactory pipelineBuilderFactory, List<ModifierProtocolInstaller<Orientation.Server>> modifiers,
            LogProvider logProvider, CatchupServerHandler catchupServerHandler )
    {
        this.pipelineBuilderFactory = pipelineBuilderFactory;
        this.modifiers = modifiers;
        this.log = logProvider.getLog( getClass() );
        this.logProvider = logProvider;
        this.catchupServerHandler = catchupServerHandler;
    }

    @Override
    public void install( Channel channel ) throws Exception
    {
        CatchupServerProtocol state = new CatchupServerProtocol();

        pipelineBuilderFactory.server( channel, log )
                .modify( modifiers )
                .addFraming()
                .add( "enc_req_type", new RequestMessageTypeEncoder() )
                .add( "enc_res_type", new ResponseMessageTypeEncoder() )
                .add( "enc_res_tx_pull", new TxPullResponseEncoder() )
                .add( "enc_res_store_id", new GetStoreIdResponseEncoder() )
                .add( "enc_res_copy_fin", new StoreCopyFinishedResponseEncoder() )
                .add( "enc_res_tx_fin", new TxStreamFinishedResponseEncoder() )
                .add( "enc_res_pre_copy", new PrepareStoreCopyResponse.Encoder() )
                .add( "enc_snapshot", new CoreSnapshotEncoder() )
                .add( "enc_file_chunk", new FileChunkEncoder() )
                .add( "enc_file_header", new FileHeaderEncoder() )
                .add( "in_req_type", serverMessageHandler( state ) )
                .add( "dec_req_dispatch", requestDecoders( state ) )
                .add( "out_chunked_write", new ChunkedWriteHandler() )
                .add( "hnd_req_tx", catchupServerHandler.txPullRequestHandler( state ) )
                .add( "hnd_req_store_id", catchupServerHandler.getStoreIdRequestHandler( state ) )
                .add( "hnd_req_store_listing", catchupServerHandler.storeListingRequestHandler( state ) )
                .add( "hnd_req_store_file", catchupServerHandler.getStoreFileRequestHandler( state ) )
                .add( "hnd_req_index_snapshot", catchupServerHandler.getIndexSnapshotRequestHandler( state ) )
                .add( "hnd_req_snapshot", catchupServerHandler.snapshotHandler( state ).map( Collections::singletonList ).orElse( emptyList() ) )
                .install();
    }

    private ChannelHandler serverMessageHandler( CatchupServerProtocol state )
    {
        return new ServerMessageTypeHandler( state, logProvider );
    }

    private ChannelInboundHandler requestDecoders( CatchupServerProtocol protocol )
    {
        RequestDecoderDispatcher<CatchupServerProtocol.State> decoderDispatcher = new RequestDecoderDispatcher<>( protocol, logProvider );
        decoderDispatcher.register( CatchupServerProtocol.State.TX_PULL, new TxPullRequestDecoder() );
        decoderDispatcher.register( CatchupServerProtocol.State.GET_STORE_ID, new SimpleRequestDecoder( GetStoreIdRequest::new ) );
        decoderDispatcher.register( CatchupServerProtocol.State.GET_CORE_SNAPSHOT, new SimpleRequestDecoder( CoreSnapshotRequest::new ) );
        decoderDispatcher.register( CatchupServerProtocol.State.PREPARE_STORE_COPY, new PrepareStoreCopyRequestDecoder() );
        decoderDispatcher.register( CatchupServerProtocol.State.GET_STORE_FILE, new GetStoreFileRequest.Decoder() );
        decoderDispatcher.register( CatchupServerProtocol.State.GET_INDEX_SNAPSHOT, new GetIndexFilesRequest.Decoder() );
        return decoderDispatcher;
    }

    @Override
    public Protocol.ApplicationProtocol applicationProtocol()
    {
        return APPLICATION_PROTOCOL;
    }

    @Override
    public Collection<Collection<Protocol.ModifierProtocol>> modifiers()
    {
        return modifiers.stream()
                .map( ModifierProtocolInstaller::protocols )
                .collect( Collectors.toList() );
    }
}
