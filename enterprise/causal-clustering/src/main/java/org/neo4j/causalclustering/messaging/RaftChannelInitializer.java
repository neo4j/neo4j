/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import org.neo4j.causalclustering.handlers.PipelineHandlerAppender;
import org.neo4j.causalclustering.messaging.marshalling.ChannelMarshal;
import org.neo4j.causalclustering.messaging.marshalling.RaftMessageEncoder;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class RaftChannelInitializer extends ChannelInitializer<SocketChannel>
{
    private final ChannelMarshal<ReplicatedContent> marshal;
    private final Log log;
    private final Monitors monitors;
    private final PipelineHandlerAppender pipelineAppender;

    public RaftChannelInitializer( ChannelMarshal<ReplicatedContent> marshal, LogProvider logProvider,
                                   Monitors monitors, PipelineHandlerAppender pipelineAppender )
    {
        this.marshal = marshal;
        this.log = logProvider.getLog( getClass() );
        this.monitors = monitors;
        this.pipelineAppender = pipelineAppender;
    }

    @Override
    protected void initChannel( SocketChannel ch ) throws Exception
    {
        ChannelPipeline pipeline = ch.pipeline();

        pipelineAppender.addPipelineHandlerForClient( pipeline, ch );

        pipeline.addLast( "frameEncoder", new LengthFieldPrepender( 4 ) );
        pipeline.addLast( new VersionPrepender() );
        pipeline.addLast( "raftMessageEncoder", new RaftMessageEncoder( marshal ) );

        pipeline.addLast( new ExceptionLoggingHandler( log ) );
        pipeline.addLast( new ExceptionMonitoringHandler(
                monitors.newMonitor( ExceptionMonitoringHandler.Monitor.class, SenderService.class ) ) );
        pipeline.addLast( new ExceptionSwallowingHandler() );
    }
}
