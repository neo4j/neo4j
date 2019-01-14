/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.upstream.strategies;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionStrategy;
import org.neo4j.helpers.Service;

@Service.Implementation( UpstreamDatabaseSelectionStrategy.class )
public class ConnectRandomlyToServerGroupStrategy extends UpstreamDatabaseSelectionStrategy
{
    static final String IDENTITY = "connect-randomly-to-server-group";
    private ConnectRandomlyToServerGroupImpl strategyImpl;

    public ConnectRandomlyToServerGroupStrategy()
    {
        super( IDENTITY );
    }

    @Override
    public void init()
    {
        List<String> groups = config.get( CausalClusteringSettings.connect_randomly_to_server_group_strategy );
        strategyImpl = new ConnectRandomlyToServerGroupImpl( groups, topologyService, myself );

        if ( groups.isEmpty() )
        {
            log.warn( "No server groups configured for upstream strategy " + readableName + ". Strategy will not find upstream servers." );
        }
        else
        {
            String readableGroups = groups.stream().collect( Collectors.joining( ", " ) );
            log.info( "Upstream selection strategy " + readableName + " configured with server groups " + readableGroups );
        }
    }

    @Override
    public Optional<MemberId> upstreamDatabase()
    {
        return strategyImpl.upstreamDatabase();
    }
}
