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
package org.neo4j.causalclustering.protocol;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;

import org.neo4j.causalclustering.handlers.PipelineWrapper;
import org.neo4j.causalclustering.protocol.ProtocolInstaller.Orientation;
import org.neo4j.logging.Log;

public class NettyPipelineBuilderFactory
{
    private final PipelineWrapper wrapper;

    public NettyPipelineBuilderFactory( PipelineWrapper wrapper )
    {
        this.wrapper = wrapper;
    }

    public ClientNettyPipelineBuilder client( Channel channel, Log log ) throws Exception
    {
        return create( channel, NettyPipelineBuilder.client( channel.pipeline(), log ) );
    }

    public ServerNettyPipelineBuilder server( Channel channel, Log log ) throws Exception
    {
        return create( channel, NettyPipelineBuilder.server( channel.pipeline(), log ) );
    }

    private <O extends Orientation, BUILDER extends NettyPipelineBuilder<O,BUILDER>> BUILDER create(
            Channel channel, BUILDER nettyPipelineBuilder ) throws Exception
    {
        int i = 0;
        for ( ChannelHandler handler : wrapper.handlersFor( channel ) )
        {
            nettyPipelineBuilder.add( String.format( "%s_%d", wrapper.name(), i ), handler );
            i++;
        }
        return nettyPipelineBuilder;
    }
}
