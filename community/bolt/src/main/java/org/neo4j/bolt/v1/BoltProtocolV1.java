/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.BoltProtocol;
import org.neo4j.bolt.runtime.BoltConnection;
import org.neo4j.bolt.runtime.BoltConnectionFactory;
import org.neo4j.bolt.runtime.BoltStateMachine;
import org.neo4j.bolt.v1.messaging.BoltMessageRouter;
import org.neo4j.bolt.v1.messaging.BoltResponseMessageWriter;
import org.neo4j.bolt.v1.messaging.Neo4jPack;
import org.neo4j.bolt.v1.messaging.Neo4jPackV1;
import org.neo4j.bolt.v1.runtime.BoltStateMachineFactory;
import org.neo4j.kernel.impl.logging.LogService;

public class BoltProtocolV1 implements BoltProtocol
{
    public static final long VERSION = 1;

    private final Neo4jPack neo4jPack;
    private final BoltMessageRouter messageRouter;
    private final BoltStateMachine stateMachine;

    private final BoltConnection connection;

    public BoltProtocolV1( BoltChannel channel, BoltConnectionFactory connectionFactory, BoltStateMachineFactory machineFactory, LogService logging )
    {
        this.neo4jPack = createPack();
        this.stateMachine = machineFactory.newStateMachine( version(), channel );
        this.connection = connectionFactory.newConnection( channel, stateMachine );
        // TODO: Require the refacoring result of router, writer, reader.
        this.messageRouter = createBoltMessageRouter( channel, neo4jPack, connection, logging, this.getClass() );
    }

    protected Neo4jPack createPack()
    {
        return new Neo4jPackV1();
    }

    @Override
    public Neo4jPack neo4jPack()
    {
        return this.neo4jPack;
    }

    @Override
    public BoltMessageRouter messageRouter()
    {
        return this.messageRouter;
    }

    @Override
    public BoltStateMachine stateMachine()
    {
        return this.stateMachine;
    }

    @Override
    public long version()
    {
        return VERSION;
    }

    @Override
    public BoltConnection connection()
    {
        return this.connection;
    }

    public static BoltMessageRouter createBoltMessageRouter( BoltChannel channel, Neo4jPack neo4jPack, BoltConnection connection, LogService logging,
            Class loggingClass )
    {
        BoltResponseMessageWriter responseWriter = new BoltResponseMessageWriter( neo4jPack, connection.output(), logging, channel.log() );
        return new BoltMessageRouter( logging.getInternalLog( loggingClass ), channel.log(), connection, responseWriter );
    }
}
