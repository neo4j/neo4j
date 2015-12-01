/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.coreedge.server.core;

import org.neo4j.coreedge.catchup.CatchupServer;
import org.neo4j.coreedge.discovery.CoreDiscoveryService;
import org.neo4j.coreedge.discovery.RaftDiscoveryServiceConnector;
import org.neo4j.coreedge.raft.RaftServer;
import org.neo4j.coreedge.raft.ScheduledTimeoutService;
import org.neo4j.coreedge.raft.replication.id.ReplicatedIdGeneratorFactory;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.lifecycle.LifeSupport;

public class CoreServerStartupProcess
{
    public static LifeSupport createLifeSupport( DataSourceManager dataSourceManager,
                                                 ReplicatedIdGeneratorFactory idGeneratorFactory,
                                                 RaftServer<CoreMember> raftServer,
                                                 CatchupServer catchupServer,
                                                 ScheduledTimeoutService raftTimeoutService,
                                                 CoreDiscoveryService coreDiscoveryService,
                                                 RaftDiscoveryServiceConnector discoveryServiceConnector,
                                                 DeleteStoreOnStartUp deleteStoreOnStartUp,
                                                 RaftLogReplay raftLogReplay,
                                                 WaitToCatchUp<CoreMember> waitToCatchup )
    {
        LifeSupport services = new LifeSupport();
        services.add( deleteStoreOnStartUp );
        services.add( dataSourceManager );
        services.add( idGeneratorFactory );
        services.add( raftLogReplay );
        services.add( raftServer );
        services.add( catchupServer );
        services.add( coreDiscoveryService );
        services.add( discoveryServiceConnector );
        services.add( raftTimeoutService );
        services.add( waitToCatchup );

        return services;
    }

}
