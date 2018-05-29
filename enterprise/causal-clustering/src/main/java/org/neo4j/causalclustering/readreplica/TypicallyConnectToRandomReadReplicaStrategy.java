/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.causalclustering.readreplica;

import java.util.Optional;

import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.Service;

@Service.Implementation( UpstreamDatabaseSelectionStrategy.class )
public class TypicallyConnectToRandomReadReplicaStrategy extends UpstreamDatabaseSelectionStrategy
{
    private final ModuloCounter counter = new ModuloCounter( 10 );

    public TypicallyConnectToRandomReadReplicaStrategy()
    {
        super( "typically-connect-to-random-read-replica" );
    }

    @Override
    public Optional<MemberId> upstreamDatabase() throws UpstreamDatabaseSelectionException
    {
        if ( counter.shouldReturnCoreMemberId() )
        {
            return topologyService.coreServers().anyCoreMemberId();
        }
        else
        {
            return topologyService.readReplicas().anyReadReplicaMemberId();
        }
    }

    private static class ModuloCounter
    {
        private final int modulo;
        private int counter = 0;

        ModuloCounter( int modulo )
        {
            // e.g. every 10th means 0-9
            this.modulo = modulo - 1;
        }

        boolean shouldReturnCoreMemberId()
        {
            counter = (counter + 1) % modulo;
            return counter == 0;
        }
    }
}
