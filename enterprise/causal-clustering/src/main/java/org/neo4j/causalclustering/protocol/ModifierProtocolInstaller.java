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

import io.netty.handler.codec.compression.SnappyFrameDecoder;
import io.netty.handler.codec.compression.SnappyFrameEncoder;

import java.util.List;

import org.neo4j.causalclustering.protocol.ProtocolInstaller.Orientation;

import static java.util.Arrays.asList;

public interface ModifierProtocolInstaller<O extends Orientation>
{
    Protocol.ModifierProtocol protocol();

    <BUILDER extends NettyPipelineBuilder<O,BUILDER>> void apply( NettyPipelineBuilder<O,BUILDER> nettyPipelineBuilder );

    List<ModifierProtocolInstaller<Orientation.Server>> serverCompressionInstallers = asList( SnappyServer.instance );
    List<ModifierProtocolInstaller<Orientation.Client>> clientCompressionInstallers = asList( SnappyClient.instance );

    List<ModifierProtocolInstaller<Orientation.Server>> allServerInstallers = serverCompressionInstallers;
    List<ModifierProtocolInstaller<Orientation.Client>> allClientInstallers = clientCompressionInstallers;

    class SnappyClient implements ModifierProtocolInstaller<Orientation.Client>
    {
        public static final SnappyClient instance = new SnappyClient();

        private SnappyClient()
        {
        }

        @Override
        public Protocol.ModifierProtocol protocol()
        {
            return Protocol.ModifierProtocols.COMPRESSION_SNAPPY;
        }

        @Override
        public <BUILDER extends NettyPipelineBuilder<Orientation.Client,BUILDER>> void apply(
                NettyPipelineBuilder<Orientation.Client,BUILDER> nettyPipelineBuilder )
        {
            nettyPipelineBuilder.add( "snappy_encoder", new SnappyFrameEncoder() );
        }
    }

    class SnappyServer implements ModifierProtocolInstaller<Orientation.Server>
    {
        private SnappyServer()
        {
        }

        public static final SnappyServer instance = new SnappyServer();

        @Override
        public Protocol.ModifierProtocol protocol()
        {
            return Protocol.ModifierProtocols.COMPRESSION_SNAPPY;
        }

        @Override
        public <BUILDER extends NettyPipelineBuilder<Orientation.Server,BUILDER>> void apply(
                NettyPipelineBuilder<Orientation.Server,BUILDER> nettyPipelineBuilder )
        {
            nettyPipelineBuilder.add( "snappy_decoder", new SnappyFrameDecoder() );
        }
    }
}
