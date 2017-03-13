/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.readreplica;

import java.util.Iterator;
import java.util.Optional;
import java.util.Random;

import org.neo4j.causalclustering.discovery.CoreTopology;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.Service;

@Service.Implementation( UpstreamDatabaseSelectionStrategy.class )
public class ConnectToRandomCoreServerStrategy extends UpstreamDatabaseSelectionStrategy
{
    private final Random random = new Random();

    public ConnectToRandomCoreServerStrategy()
    {
        super( "connect-to-random-core-server" );
    }

    @Override
    public Optional<MemberId> upstreamDatabase() throws UpstreamDatabaseSelectionException
    {
        final CoreTopology coreTopology = topologyService.coreServers();

        if ( coreTopology.members().size() == 0 )
        {
            throw new UpstreamDatabaseSelectionException( "No core servers available" );
        }

        int skippedServers = random.nextInt( coreTopology.members().size() );

        final Iterator<MemberId> iterator = coreTopology.members().keySet().iterator();

        MemberId member;
        do
        {
            member = iterator.next();
        }
        while ( skippedServers-- > 0 );

        return Optional.ofNullable( member );
    }
}
