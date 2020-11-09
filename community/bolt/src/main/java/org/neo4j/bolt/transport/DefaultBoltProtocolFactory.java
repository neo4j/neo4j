/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.time.Duration;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.BoltProtocol;
import org.neo4j.bolt.BoltProtocolVersion;
import org.neo4j.bolt.dbapi.CustomBookmarkFormatParser;
import org.neo4j.bolt.runtime.BoltConnectionFactory;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachineFactory;
import org.neo4j.bolt.v3.BoltProtocolV3;
import org.neo4j.bolt.v4.BoltProtocolV4;
import org.neo4j.bolt.v4.runtime.bookmarking.BookmarksParserV4;
import org.neo4j.bolt.v41.BoltProtocolV41;
import org.neo4j.bolt.v42.BoltProtocolV42;
import org.neo4j.bolt.v43.BoltProtocolV43;
import org.neo4j.configuration.Config;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.logging.internal.LogService;
import org.neo4j.time.SystemNanoClock;

public class DefaultBoltProtocolFactory implements BoltProtocolFactory
{
    private final BoltConnectionFactory connectionFactory;
    private final Config config;
    private final LogService logService;
    private final BoltStateMachineFactory stateMachineFactory;
    private final BookmarksParserV4 bookmarksParserV4;
    private final SystemNanoClock clock;
    private final Duration keepAliveInterval;
    private final TransportThrottleGroup throttleGroup;

    public DefaultBoltProtocolFactory( BoltConnectionFactory connectionFactory, BoltStateMachineFactory stateMachineFactory,
            Config config, LogService logService, DatabaseIdRepository databaseIdRepository,
            CustomBookmarkFormatParser customBookmarkFormatParser, TransportThrottleGroup throttleGroup,
            SystemNanoClock clock, Duration keepAliveInterval )
    {
        this.connectionFactory = connectionFactory;
        this.stateMachineFactory = stateMachineFactory;
        this.config = config;
        this.logService = logService;
        this.bookmarksParserV4 = new BookmarksParserV4( databaseIdRepository, customBookmarkFormatParser );
        this.throttleGroup = throttleGroup;
        this.clock = clock;
        this.keepAliveInterval = keepAliveInterval;
    }

    @Override
    public BoltProtocol create( BoltProtocolVersion protocolVersion, BoltChannel channel )
    {
        if ( protocolVersion.equals( BoltProtocolV3.VERSION ) )
        {
            return new BoltProtocolV3( channel, connectionFactory, stateMachineFactory, config, logService, throttleGroup );
        }
        else if ( protocolVersion.equals( BoltProtocolV4.VERSION ) )
        {
            return new BoltProtocolV4( channel, connectionFactory, stateMachineFactory, config, bookmarksParserV4, logService,
                    throttleGroup );
        }
        else if ( protocolVersion.equals( BoltProtocolV41.VERSION ) )
        {
            return new BoltProtocolV41( channel, connectionFactory, stateMachineFactory, config, bookmarksParserV4, logService,
                                        throttleGroup, clock, keepAliveInterval );
        }
        else if ( protocolVersion.equals( BoltProtocolV42.VERSION ) )
        {
            return new BoltProtocolV42( channel, connectionFactory, stateMachineFactory, config, bookmarksParserV4, logService,
                                        throttleGroup, clock, keepAliveInterval );
        }
        else if ( protocolVersion.equals( BoltProtocolV43.VERSION ) )
        {
            return new BoltProtocolV43( channel, connectionFactory, stateMachineFactory, config, bookmarksParserV4, logService,
                                        throttleGroup, clock, keepAliveInterval );
        }
        else
        {
            return null;
        }
    }
}
