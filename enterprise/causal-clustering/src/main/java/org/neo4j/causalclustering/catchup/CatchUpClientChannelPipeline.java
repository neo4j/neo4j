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
package org.neo4j.causalclustering.catchup;

import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

import org.neo4j.causalclustering.VersionDecoder;
import org.neo4j.causalclustering.VersionPrepender;
import org.neo4j.causalclustering.catchup.storecopy.FileChunkDecoder;
import org.neo4j.causalclustering.catchup.storecopy.FileChunkHandler;
import org.neo4j.causalclustering.catchup.storecopy.FileHeaderDecoder;
import org.neo4j.causalclustering.catchup.storecopy.FileHeaderHandler;
import org.neo4j.causalclustering.catchup.storecopy.GetStoreIdRequestEncoder;
import org.neo4j.causalclustering.catchup.storecopy.GetStoreIdResponseDecoder;
import org.neo4j.causalclustering.catchup.storecopy.GetStoreIdResponseHandler;
import org.neo4j.causalclustering.catchup.storecopy.GetStoreRequestEncoder;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponseDecoder;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponseHandler;
import org.neo4j.causalclustering.catchup.tx.TxPullRequestEncoder;
import org.neo4j.causalclustering.catchup.tx.TxPullResponseDecoder;
import org.neo4j.causalclustering.catchup.tx.TxPullResponseHandler;
import org.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponseDecoder;
import org.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponseHandler;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshotDecoder;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshotRequestEncoder;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshotResponseHandler;
import org.neo4j.causalclustering.handlers.ExceptionLoggingHandler;
import org.neo4j.causalclustering.handlers.ExceptionMonitoringHandler;
import org.neo4j.causalclustering.handlers.ExceptionSwallowingHandler;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.ssl.SslPolicy;

class CatchUpClientChannelPipeline
{
    static void initChannel( SocketChannel ch, CatchUpResponseHandler handler, LogProvider logProvider, Monitors monitors, SslPolicy sslPolicy )
            throws Exception
    {
        CatchupClientProtocol protocol = new CatchupClientProtocol();

        ChannelPipeline pipeline = ch.pipeline();

        if ( sslPolicy != null )
        {
            pipeline.addLast( sslPolicy.nettyClientHandler( ch ) );
        }

        pipeline.addLast( new LengthFieldBasedFrameDecoder( Integer.MAX_VALUE, 0, 4, 0, 4 ) );
        pipeline.addLast( new LengthFieldPrepender( 4 ) );

        pipeline.addLast( new VersionDecoder( logProvider ) );
        pipeline.addLast( new VersionPrepender() );

        pipeline.addLast( new TxPullRequestEncoder() );
        pipeline.addLast( new GetStoreRequestEncoder() );
        pipeline.addLast( new CoreSnapshotRequestEncoder() );
        pipeline.addLast( new GetStoreIdRequestEncoder() );
        pipeline.addLast( new ResponseMessageTypeEncoder() );
        pipeline.addLast( new RequestMessageTypeEncoder() );

        pipeline.addLast( new ClientMessageTypeHandler( protocol, logProvider ) );

        RequestDecoderDispatcher<CatchupClientProtocol.State> decoderDispatcher =
                new RequestDecoderDispatcher<>( protocol, logProvider );
        decoderDispatcher.register( CatchupClientProtocol.State.STORE_ID, new GetStoreIdResponseDecoder() );
        decoderDispatcher.register( CatchupClientProtocol.State.TX_PULL_RESPONSE, new TxPullResponseDecoder() );
        decoderDispatcher.register( CatchupClientProtocol.State.CORE_SNAPSHOT, new CoreSnapshotDecoder() );
        decoderDispatcher.register( CatchupClientProtocol.State.STORE_COPY_FINISHED, new
                StoreCopyFinishedResponseDecoder() );
        decoderDispatcher.register( CatchupClientProtocol.State.TX_STREAM_FINISHED, new
                TxStreamFinishedResponseDecoder() );
        decoderDispatcher.register( CatchupClientProtocol.State.FILE_HEADER, new FileHeaderDecoder() );
        decoderDispatcher.register( CatchupClientProtocol.State.FILE_CONTENTS, new FileChunkDecoder() );

        pipeline.addLast( decoderDispatcher );

        pipeline.addLast( new TxPullResponseHandler( protocol, handler ) );
        pipeline.addLast( new CoreSnapshotResponseHandler( protocol, handler ) );
        pipeline.addLast( new StoreCopyFinishedResponseHandler( protocol, handler ) );
        pipeline.addLast( new TxStreamFinishedResponseHandler( protocol, handler ) );
        pipeline.addLast( new FileHeaderHandler( protocol, handler, logProvider ) );
        pipeline.addLast( new FileChunkHandler( protocol, handler ) );
        pipeline.addLast( new GetStoreIdResponseHandler( protocol, handler ) );

        pipeline.addLast( new ExceptionLoggingHandler( logProvider.getLog( CatchUpClient.class ) ) );
        pipeline.addLast( new ExceptionMonitoringHandler(
                monitors.newMonitor( ExceptionMonitoringHandler.Monitor.class, CatchUpClient.class ) ) );
        pipeline.addLast( new ExceptionSwallowingHandler() );
    }
}
