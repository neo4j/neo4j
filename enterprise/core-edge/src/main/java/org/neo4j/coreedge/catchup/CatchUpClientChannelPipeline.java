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

import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

import org.neo4j.coreedge.VersionDecoder;
import org.neo4j.coreedge.VersionPrepender;
import org.neo4j.coreedge.catchup.storecopy.FileContentDecoder;
import org.neo4j.coreedge.catchup.storecopy.FileContentHandler;
import org.neo4j.coreedge.catchup.storecopy.FileHeaderDecoder;
import org.neo4j.coreedge.catchup.storecopy.FileHeaderHandler;
import org.neo4j.coreedge.catchup.storecopy.GetStoreIdRequestEncoder;
import org.neo4j.coreedge.catchup.storecopy.GetStoreIdResponseDecoder;
import org.neo4j.coreedge.catchup.storecopy.GetStoreIdResponseHandler;
import org.neo4j.coreedge.catchup.storecopy.GetStoreRequestEncoder;
import org.neo4j.coreedge.catchup.storecopy.StoreCopyFinishedResponseDecoder;
import org.neo4j.coreedge.catchup.storecopy.StoreCopyFinishedResponseHandler;
import org.neo4j.coreedge.catchup.tx.TxPullRequestEncoder;
import org.neo4j.coreedge.catchup.tx.TxPullResponseDecoder;
import org.neo4j.coreedge.catchup.tx.TxPullResponseHandler;
import org.neo4j.coreedge.catchup.tx.TxStreamFinishedResponseDecoder;
import org.neo4j.coreedge.catchup.tx.TxStreamFinishedResponseHandler;
import org.neo4j.coreedge.core.state.snapshot.CoreSnapshotDecoder;
import org.neo4j.coreedge.core.state.snapshot.CoreSnapshotRequestEncoder;
import org.neo4j.coreedge.core.state.snapshot.CoreSnapshotResponseHandler;
import org.neo4j.coreedge.logging.ExceptionLoggingHandler;
import org.neo4j.logging.LogProvider;

class CatchUpClientChannelPipeline
{
    static void initChannel( SocketChannel ch, CatchUpResponseHandler handler, LogProvider logProvider )
            throws Exception
    {
        CatchupClientProtocol protocol = new CatchupClientProtocol();

        ChannelPipeline pipeline = ch.pipeline();
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
        decoderDispatcher.register( CatchupClientProtocol.State.FILE_CONTENTS, new FileContentDecoder() );

        pipeline.addLast( decoderDispatcher );

        pipeline.addLast( new TxPullResponseHandler( protocol, handler ) );
        pipeline.addLast( new CoreSnapshotResponseHandler( protocol, handler ) );
        pipeline.addLast( new StoreCopyFinishedResponseHandler( protocol, handler ) );
        pipeline.addLast( new TxStreamFinishedResponseHandler( protocol, handler ) );
        pipeline.addLast( new FileHeaderHandler( protocol, handler, logProvider ) );
        pipeline.addLast( new FileContentHandler( protocol, handler ) );
        pipeline.addLast( new GetStoreIdResponseHandler( protocol, handler ) );

        pipeline.addLast( new ExceptionLoggingHandler( logProvider.getLog( CatchUpClient.class ) ) );
    }
}
