/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.upstream.strategies;

import java.util.Optional;

import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionStrategy;
import org.neo4j.helpers.Service;

@Service.Implementation( UpstreamDatabaseSelectionStrategy.class )
public class TypicallyConnectToRandomReadReplicaStrategy extends UpstreamDatabaseSelectionStrategy
{
    public static final String IDENTITY = "typically-connect-to-random-read-replica";
    private final ModuloCounter counter;

    public TypicallyConnectToRandomReadReplicaStrategy()
    {
        this( 10 );
    }

    public TypicallyConnectToRandomReadReplicaStrategy( int connectToCoreInterval )
    {
        super( IDENTITY );
        this.counter = new ModuloCounter( connectToCoreInterval );
    }

    @Override
    public Optional<MemberId> upstreamDatabase()
    {
        if ( counter.shouldReturnCoreMemberId() )
        {
            return getCoreMemberId();
        }
        else
        {
            Optional<MemberId> memberId = getReadReplicaMemberId();
            if ( !memberId.isPresent() )
            {
                memberId = getCoreMemberId();
            }
            return memberId;
        }
    }

    private Optional<MemberId> getReadReplicaMemberId()
    {
        return topologyService.localReadReplicas().randomReadReplicaMemberId();
    }

    private Optional<MemberId> getCoreMemberId()
    {
        return topologyService.localCoreServers().randomCoreMemberId();
    }

    private static class ModuloCounter
    {
        private final int modulo;
        private int counter;

        ModuloCounter( int modulo )
        {
            this.modulo = modulo;
        }

        boolean shouldReturnCoreMemberId()
        {
            counter = (counter + 1) % modulo;
            return counter == 0;
        }
    }
}
