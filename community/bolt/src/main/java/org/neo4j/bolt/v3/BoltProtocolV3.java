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
package org.neo4j.bolt.v3;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.messaging.BoltRequestMessageReader;
import org.neo4j.bolt.messaging.Neo4jPack;
import org.neo4j.bolt.runtime.BoltConnection;
import org.neo4j.bolt.runtime.BoltConnectionFactory;
import org.neo4j.bolt.runtime.BoltStateMachineFactory;
import org.neo4j.bolt.v1.BoltProtocolV1;
import org.neo4j.bolt.v1.messaging.BoltResponseMessageWriterV1;
import org.neo4j.bolt.v2.messaging.Neo4jPackV2;
import org.neo4j.bolt.v3.messaging.BoltRequestMessageReaderV3;
import org.neo4j.logging.internal.LogService;

/**
 * Bolt protocol V3. It hosts all the components that are specific to BoltV3
 */
public class BoltProtocolV3 extends BoltProtocolV1
{
    public static final long VERSION = 3;

    public BoltProtocolV3( BoltChannel channel, BoltConnectionFactory connectionFactory, BoltStateMachineFactory stateMachineFactory, LogService logging )
    {
        super( channel, connectionFactory, stateMachineFactory, logging );
    }

    @Override
    protected Neo4jPack createPack()
    {
        return new Neo4jPackV2();
    }

    @Override
    public long version()
    {
        return VERSION;
    }

    @Override
    protected BoltRequestMessageReader createMessageReader( BoltChannel channel, Neo4jPack neo4jPack, BoltConnection connection, LogService logging )
    {
        BoltResponseMessageWriterV1 responseWriter = new BoltResponseMessageWriterV1( neo4jPack, connection.output(), logging );
        return new BoltRequestMessageReaderV3( connection, responseWriter, logging );
    }
}
