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
package org.neo4j.causalclustering.messaging;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldPrepender;

import org.neo4j.causalclustering.VersionPrepender;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.handlers.ExceptionLoggingHandler;
import org.neo4j.causalclustering.handlers.ExceptionMonitoringHandler;
import org.neo4j.causalclustering.handlers.ExceptionSwallowingHandler;
import org.neo4j.causalclustering.messaging.marshalling.ChannelMarshal;
import org.neo4j.causalclustering.messaging.marshalling.RaftMessageEncoder;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.ssl.SslPolicy;

public class RaftChannelInitializer extends ChannelInitializer<SocketChannel>
{
    private final ChannelMarshal<ReplicatedContent> marshal;
    private final Log log;
    private final Monitors monitors;
    private final SslPolicy sslPolicy;

    public RaftChannelInitializer( ChannelMarshal<ReplicatedContent> marshal, LogProvider logProvider,
            Monitors monitors, SslPolicy sslPolicy )
    {
        this.marshal = marshal;
        this.log = logProvider.getLog( getClass() );
        this.monitors = monitors;
        this.sslPolicy = sslPolicy;
    }

    @Override
    protected void initChannel( SocketChannel ch ) throws Exception
    {
        ChannelPipeline pipeline = ch.pipeline();

        if ( sslPolicy != null )
        {
            pipeline.addLast( sslPolicy.nettyClientHandler( ch ) );
        }

        pipeline.addLast( "frameEncoder", new LengthFieldPrepender( 4 ) );
        pipeline.addLast( new VersionPrepender() );
        pipeline.addLast( "raftMessageEncoder", new RaftMessageEncoder( marshal ) );

        pipeline.addLast( new ExceptionLoggingHandler( log ) );
        pipeline.addLast( new ExceptionMonitoringHandler(
                monitors.newMonitor( ExceptionMonitoringHandler.Monitor.class, SenderService.class ) ) );
        pipeline.addLast( new ExceptionSwallowingHandler() );
    }
}
