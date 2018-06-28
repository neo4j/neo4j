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
package org.neo4j.bolt.transport;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.messaging.Neo4jPack;
import org.neo4j.bolt.runtime.BoltConnection;
import org.neo4j.bolt.runtime.BoltConnectionFactory;
import org.neo4j.bolt.BoltProtocol;
import org.neo4j.bolt.v1.BoltProtocolV1;
import org.neo4j.bolt.v1.runtime.BoltStateMachineFactory;
import org.neo4j.bolt.v2.BoltProtocolV2;
import org.neo4j.kernel.impl.logging.LogService;

public class DefaultBoltProtocolPipelineInstallerFactory implements BoltProtocolPipelineInstallerFactory
{
    private final BoltConnectionFactory connectionFactory;
    private final LogService logService;
    private final BoltStateMachineFactory stateMachineFactory;

    public DefaultBoltProtocolPipelineInstallerFactory( BoltConnectionFactory connectionFactory, BoltStateMachineFactory stateMachineFactory,
            LogService logService )
    {
        this.connectionFactory = connectionFactory;
        this.stateMachineFactory = stateMachineFactory;
        this.logService = logService;
    }

    @Override
    public BoltProtocolPipelineInstaller create( long protocolVersion, BoltChannel channel )
    {
        BoltProtocol boltProtocol;
        if ( protocolVersion == BoltProtocolV1.VERSION )
        {
            boltProtocol = new BoltProtocolV1( channel, connectionFactory, stateMachineFactory, logService );
        }
        else if ( protocolVersion == BoltProtocolV2.VERSION )
        {
            boltProtocol = new BoltProtocolV2( channel, connectionFactory, stateMachineFactory, logService );
        }
        else
        {
            return null;
        }
        return new DefaultBoltProtocolPipelineInstaller( channel, boltProtocol, logService );
    }
}
