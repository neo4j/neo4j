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
package org.neo4j.bolt.v1;

import io.netty.channel.ChannelPipeline;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.BoltProtocol;
import org.neo4j.bolt.messaging.BoltRequestMessageReader;
import org.neo4j.bolt.messaging.Neo4jPack;
import org.neo4j.bolt.runtime.BoltConnection;
import org.neo4j.bolt.runtime.BoltConnectionFactory;
import org.neo4j.bolt.runtime.BoltStateMachine;
import org.neo4j.bolt.runtime.BoltStateMachineFactory;
import org.neo4j.bolt.transport.pipeline.ChunkDecoder;
import org.neo4j.bolt.transport.pipeline.HouseKeeper;
import org.neo4j.bolt.transport.pipeline.MessageAccumulator;
import org.neo4j.bolt.transport.pipeline.MessageDecoder;
import org.neo4j.bolt.v1.messaging.BoltRequestMessageReaderV1;
import org.neo4j.bolt.v1.messaging.BoltResponseMessageWriterV1;
import org.neo4j.bolt.v1.messaging.Neo4jPackV1;
import org.neo4j.logging.internal.LogService;

/**
 * Bolt protocol V1. It hosts all the components that are specific to BoltV1
 */
public class BoltProtocolV1 implements BoltProtocol
{
    public static final long VERSION = 1;

    private final Neo4jPack neo4jPack;
    private final BoltConnection connection;
    private final BoltRequestMessageReader messageReader;

    private final BoltChannel channel;
    private final LogService logging;

    public BoltProtocolV1( BoltChannel channel, BoltConnectionFactory connectionFactory, BoltStateMachineFactory stateMachineFactory, LogService logging )
    {
        this.channel = channel;
        this.logging = logging;

        BoltStateMachine stateMachine = stateMachineFactory.newStateMachine( version(), channel );
        this.connection = connectionFactory.newConnection( channel, stateMachine );

        this.neo4jPack = createPack();
        this.messageReader = createMessageReader( channel, neo4jPack, connection, logging );
    }

    /**
     * Install chunker, packstream, message reader, message handler, message encoder for protocol v1
     */
    @Override
    public void install()
    {
        ChannelPipeline pipeline = channel.rawChannel().pipeline();

        pipeline.addLast( new ChunkDecoder() );
        pipeline.addLast( new MessageAccumulator() );
        pipeline.addLast( new MessageDecoder( neo4jPack, messageReader, logging ) );
        pipeline.addLast( new HouseKeeper( connection, logging.getInternalLog( HouseKeeper.class ) ) );
    }

    protected Neo4jPack createPack()
    {
        return new Neo4jPackV1();
    }

    @Override
    public long version()
    {
        return VERSION;
    }

    protected BoltRequestMessageReader createMessageReader( BoltChannel channel, Neo4jPack neo4jPack, BoltConnection connection, LogService logging )
    {
        BoltResponseMessageWriterV1 responseWriter = new BoltResponseMessageWriterV1( neo4jPack, connection.output(), logging );
        return new BoltRequestMessageReaderV1( connection, responseWriter, logging );
    }
}
