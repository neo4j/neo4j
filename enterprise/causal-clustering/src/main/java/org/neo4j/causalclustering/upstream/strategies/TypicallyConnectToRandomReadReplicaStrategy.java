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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
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
        List<MemberId> coreMembersNotSelf =
                topologyService.localCoreServers().members().keySet().stream().filter( not( myself::equals ) ).collect( Collectors.toList() );
        Collections.shuffle( coreMembersNotSelf );
        if ( coreMembersNotSelf.size() == 0 )
        {
            return Optional.empty();
        }
        return Optional.of( coreMembersNotSelf.get( 0 ) );
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
