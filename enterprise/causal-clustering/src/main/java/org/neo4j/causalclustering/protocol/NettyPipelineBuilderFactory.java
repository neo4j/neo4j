/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
