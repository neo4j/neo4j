/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.upstream.strategies;

import java.util.List;
import java.util.Optional;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionStrategy;
import org.neo4j.helpers.Service;

@Service.Implementation( UpstreamDatabaseSelectionStrategy.class )
public class ConnectRandomlyWithinServerGroupStrategy extends UpstreamDatabaseSelectionStrategy
{
    public static final String IDENTITY = "connect-randomly-within-server-group";
    private ConnectRandomlyToServerGroupImpl strategyImpl;

    public ConnectRandomlyWithinServerGroupStrategy()
    {
        super( IDENTITY );
    }

    @Override
    public void init()
    {
        List<String> groups = config.get( CausalClusteringSettings.server_groups );
        strategyImpl = new ConnectRandomlyToServerGroupImpl( groups, topologyService, myself );
        log.warn( "Upstream selection strategy " + readableName + " is deprecated. Consider using " + IDENTITY + " instead." );
    }

    @Override
    public Optional<MemberId> upstreamDatabase()
    {
        return strategyImpl.upstreamDatabase();
    }
}
