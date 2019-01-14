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

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.runtime.BoltConnection;
import org.neo4j.bolt.runtime.BoltConnectionFactory;
import org.neo4j.bolt.v1.messaging.Neo4jPack;
import org.neo4j.bolt.v1.messaging.Neo4jPackV1;
import org.neo4j.bolt.v2.messaging.Neo4jPackV2;
import org.neo4j.kernel.impl.logging.LogService;

public class DefaultBoltProtocolPipelineInstallerFactory implements BoltProtocolPipelineInstallerFactory
{
    private final BoltConnectionFactory connectionFactory;
    private final TransportThrottleGroup throttleGroup;
    private final LogService logService;

    public DefaultBoltProtocolPipelineInstallerFactory( BoltConnectionFactory connectionFactory, TransportThrottleGroup throttleGroup,
            LogService logService )
    {
        this.connectionFactory = connectionFactory;
        this.throttleGroup = throttleGroup;
        this.logService = logService;
    }

    @Override
    public BoltProtocolPipelineInstaller create( long protocolVersion, BoltChannel channel )
    {
        if ( protocolVersion == Neo4jPackV1.VERSION )
        {
            return newProtocolPipelineInstaller( channel, new Neo4jPackV1() );
        }
        else if ( protocolVersion == Neo4jPackV2.VERSION )
        {
            return newProtocolPipelineInstaller( channel, new Neo4jPackV2() );
        }
        else
        {
            return null;
        }
    }

    private BoltProtocolPipelineInstaller newProtocolPipelineInstaller( BoltChannel channel, Neo4jPack neo4jPack )
    {
        return new DefaultBoltProtocolPipelineInstaller( channel, newBoltConnection( channel ), neo4jPack, throttleGroup, logService );
    }

    private BoltConnection newBoltConnection( BoltChannel channel )
    {
        return connectionFactory.newConnection( channel );
    }
}
