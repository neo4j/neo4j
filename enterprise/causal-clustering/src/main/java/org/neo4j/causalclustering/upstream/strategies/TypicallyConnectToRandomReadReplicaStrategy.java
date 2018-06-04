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
package org.neo4j.causalclustering.upstream.strategies;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionStrategy;
import org.neo4j.helpers.Service;

import static org.neo4j.function.Predicates.not;

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
        this.myself = myself;
    }

    @Override
    public Optional<MemberId> upstreamDatabase()
    {
        if ( counter.shouldReturnCoreMemberId() )
        {
            return randomCoreMember();
        }
        else
        {
            // shuffled members
            List<MemberId> readReplicaMembers = new ArrayList<>( topologyService.localReadReplicas().members().keySet() );
            Collections.shuffle( readReplicaMembers );

            List<MemberId> coreMembers = new ArrayList<>( topologyService.localCoreServers().members().keySet() );
            Collections.shuffle( coreMembers );

            return Stream.concat( readReplicaMembers.stream(), coreMembers.stream() ).filter( not( myself::equals ) ).findFirst();
        }
    }

    private Optional<MemberId> randomCoreMember()
    {
        return topologyService.localCoreServers().members().keySet().stream().filter( not( myself::equals ) ).findFirst();
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
