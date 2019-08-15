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
import org.neo4j.bolt.BoltProtocol;
import org.neo4j.bolt.messaging.BoltRequestMessageReader;
import org.neo4j.bolt.packstream.Neo4jPack;
import org.neo4j.bolt.runtime.BoltConnection;
import org.neo4j.bolt.runtime.BoltConnectionFactory;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachine;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachineFactory;
import org.neo4j.bolt.runtime.BookmarksParser;
import org.neo4j.bolt.transport.pipeline.ChunkDecoder;
import org.neo4j.bolt.transport.pipeline.HouseKeeper;
import org.neo4j.bolt.transport.pipeline.MessageAccumulator;
import org.neo4j.bolt.transport.pipeline.MessageDecoder;
import org.neo4j.bolt.v3.runtime.bookmarking.BookmarksParserV3;
import org.neo4j.logging.internal.LogService;

/**
 * The base of building Bolt protocols.
 */
public abstract class AbstractBoltProtocol implements BoltProtocol
{
    private final Neo4jPack neo4jPack;
    private final BoltConnection connection;
    private final BoltRequestMessageReader messageReader;

    private final BoltChannel channel;
    private final LogService logging;

    public AbstractBoltProtocol( BoltChannel channel, BoltConnectionFactory connectionFactory, BoltStateMachineFactory stateMachineFactory, LogService logging )
    {
        this( channel, connectionFactory, stateMachineFactory, BookmarksParserV3.INSTANCE, logging );
    }

    protected AbstractBoltProtocol( BoltChannel channel, BoltConnectionFactory connectionFactory, BoltStateMachineFactory stateMachineFactory,
            BookmarksParser bookmarksParser, LogService logging )
    {
        this.channel = channel;
        this.logging = logging;

        BoltStateMachine stateMachine = stateMachineFactory.newStateMachine( version(), channel );
        this.connection = connectionFactory.newConnection( channel, stateMachine );

        this.neo4jPack = createPack();
        this.messageReader = createMessageReader( channel, neo4jPack, connection, bookmarksParser, logging );
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

    protected abstract Neo4jPack createPack();

    protected abstract BoltRequestMessageReader createMessageReader( BoltChannel channel, Neo4jPack neo4jPack, BoltConnection connection,
            BookmarksParser bookmarksParser, LogService logging );
}
