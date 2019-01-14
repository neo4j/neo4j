/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.transport;

import io.netty.channel.ChannelPipeline;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.runtime.BoltConnection;
import org.neo4j.bolt.transport.pipeline.ChunkDecoder;
import org.neo4j.bolt.transport.pipeline.HouseKeeper;
import org.neo4j.bolt.transport.pipeline.MessageAccumulator;
import org.neo4j.bolt.transport.pipeline.MessageDecoder;
import org.neo4j.bolt.v1.messaging.BoltMessageRouter;
import org.neo4j.bolt.v1.messaging.BoltRequestMessageHandler;
import org.neo4j.bolt.v1.messaging.BoltResponseMessageWriter;
import org.neo4j.bolt.v1.messaging.Neo4jPack;
import org.neo4j.kernel.impl.logging.LogService;

/**
 * Implements version one of the Bolt Protocol when transported over a socket. This means this class will handle a
 * simple message framing protocol and forward messages to the messaging protocol implementation, version 1.
 * <p/>
 * Versions of the framing protocol are lock-step with the messaging protocol versioning.
 */
public class DefaultBoltProtocolPipelineInstaller implements BoltProtocolPipelineInstaller
{
    private final BoltChannel boltChannel;
    private final Neo4jPack neo4jPack;
    private final BoltResponseMessageWriter responseWriter;
    private final BoltRequestMessageHandler messageHandler;
    private final LogService logging;

    private final BoltConnection connection;

    public DefaultBoltProtocolPipelineInstaller( BoltChannel boltChannel, BoltConnection connection, Neo4jPack neo4jPack, TransportThrottleGroup throttleGroup,
            LogService logging )
    {
        this.boltChannel = boltChannel;
        this.connection = connection;
        this.neo4jPack = neo4jPack;
        this.responseWriter = new BoltResponseMessageWriter( neo4jPack, connection.output(), logging, boltChannel.log() );
        this.messageHandler = new BoltMessageRouter( logging.getInternalLog( getClass() ), boltChannel.log(), connection, responseWriter );
        this.logging = logging;
    }

    public void install()
    {
        ChannelPipeline pipeline = boltChannel.rawChannel().pipeline();

        pipeline.addLast( new ChunkDecoder() );
        pipeline.addLast( new MessageAccumulator() );
        pipeline.addLast( new MessageDecoder( neo4jPack, messageHandler, logging ) );
        pipeline.addLast( new HouseKeeper( connection, logging.getInternalLog( HouseKeeper.class ) ) );
    }

    @Override
    public long version()
    {
        return neo4jPack.version();
    }
}
