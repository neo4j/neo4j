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
import io.netty.handler.timeout.IdleStateHandler;

import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.neo4j.coreedge.catchup.storecopy.FileContentHandler;
import org.neo4j.coreedge.catchup.storecopy.FileHeaderHandler;
import org.neo4j.coreedge.catchup.storecopy.GetStoreRequestEncoder;
import org.neo4j.coreedge.catchup.storecopy.StoreCopyFinishedResponseHandler;
import org.neo4j.coreedge.catchup.tx.TxPullRequestEncoder;
import org.neo4j.coreedge.catchup.tx.TxPullResponseHandler;
import org.neo4j.coreedge.catchup.tx.TxStreamFinishedResponseHandler;
import org.neo4j.coreedge.core.state.snapshot.CoreSnapshotRequestEncoder;
import org.neo4j.coreedge.core.state.snapshot.CoreSnapshotResponseHandler;
import org.neo4j.coreedge.discovery.CoreTopologyService;
import org.neo4j.coreedge.logging.ExceptionLoggingHandler;
import org.neo4j.coreedge.messaging.IdleChannelReaperHandler;
import org.neo4j.coreedge.messaging.Message;
import org.neo4j.coreedge.messaging.NonBlockingChannels;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;

import static org.neo4j.coreedge.messaging.Message.CURRENT_VERSION;

public class CoreToCoreClient extends CoreClient
{
    public CoreToCoreClient( LogProvider logProvider, ChannelInitializer channelInitializer, Monitors monitors,
            int maxQueueSize, NonBlockingChannels nonBlockingChannels, CoreTopologyService discoveryService,
            long logThresholdMillis )
    {
        super( logProvider, channelInitializer, monitors, maxQueueSize, nonBlockingChannels, discoveryService,
                logThresholdMillis );
    }

    public static class ChannelInitializer extends io.netty.channel.ChannelInitializer<SocketChannel>
    {
        private final LogProvider logProvider;
        private NonBlockingChannels nonBlockingChannels;
        private CoreToCoreClient owner;

        public ChannelInitializer( LogProvider logProvider, NonBlockingChannels nonBlockingChannels )
        {
            this.logProvider = logProvider;
            this.nonBlockingChannels = nonBlockingChannels;
        }

        public void setOwner( CoreToCoreClient coreToCoreClient )
        {
            this.owner = coreToCoreClient;
        }

        @Override
        protected void initChannel( SocketChannel ch ) throws Exception
        {
            CatchupClientProtocol protocol = new CatchupClientProtocol();

            ChannelPipeline pipeline = ch.pipeline();
            pipeline.addLast( new LengthFieldBasedFrameDecoder( Integer.MAX_VALUE, 0, 4, 0, 4 ) );
            pipeline.addLast( new LengthFieldPrepender( 4 ) );

            pipeline.addLast( new TxPullRequestEncoder() );
            pipeline.addLast( new GetStoreRequestEncoder() );
            pipeline.addLast( new CoreSnapshotRequestEncoder() );
            pipeline.addLast( new ResponseMessageType.Encoder() );
            pipeline.addLast( new RequestMessageType.Encoder() );

            pipeline.addLast( new ClientMessageTypeHandler( protocol, logProvider ) );

            pipeline.addLast( owner.decoders( protocol ) );

            Predicate<Message> versionChecker = (m) -> m.version() == CURRENT_VERSION;
            pipeline.addLast( new TxPullResponseHandler( versionChecker, protocol, owner, logProvider ) );
            pipeline.addLast( new CoreSnapshotResponseHandler( versionChecker, protocol, owner, logProvider ) );
            pipeline.addLast( new StoreCopyFinishedResponseHandler( versionChecker, protocol, owner, logProvider ) );
            pipeline.addLast( new TxStreamFinishedResponseHandler( versionChecker, protocol, owner, logProvider ) );
            pipeline.addLast( new FileHeaderHandler( versionChecker, protocol, logProvider ) );
            pipeline.addLast( new FileContentHandler( versionChecker, protocol, owner, logProvider ) );

            pipeline.addLast( new IdleStateHandler( 0, 0, 2, TimeUnit.MINUTES) );
            pipeline.addLast( new IdleChannelReaperHandler(nonBlockingChannels));

            pipeline.addLast( new ExceptionLoggingHandler( logProvider.getLog( getClass() ) ) );
        }
    }
}
