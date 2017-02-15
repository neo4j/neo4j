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
package org.neo4j.causalclustering.discovery.procedures;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.neo4j.causalclustering.core.consensus.LeaderLocator;
import org.neo4j.causalclustering.core.consensus.NoLeaderFoundException;
import org.neo4j.causalclustering.discovery.ClientConnectorAddresses;
import org.neo4j.causalclustering.discovery.CoreServerInfo;
import org.neo4j.causalclustering.discovery.CoreTopology;
import org.neo4j.causalclustering.discovery.ReadReplicaInfo;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.collection.RawIterator;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.Context;
import org.neo4j.kernel.api.proc.Neo4jTypes;
import org.neo4j.kernel.api.proc.QualifiedName;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.Comparator.comparing;
import static org.neo4j.helpers.collection.Iterators.asRawIterator;
import static org.neo4j.helpers.collection.Iterators.map;
import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureSignature;

public class ClusterOverviewProcedure extends CallableProcedure.BasicProcedure
{
    private static final String[] PROCEDURE_NAMESPACE = {"dbms", "cluster"};
    public static final String PROCEDURE_NAME = "overview";
    private final TopologyService topologyService;
    private final LeaderLocator leaderLocator;
    private final Log log;

    public ClusterOverviewProcedure( TopologyService topologyService,
            LeaderLocator leaderLocator, LogProvider logProvider )
    {
        super( procedureSignature( new QualifiedName( PROCEDURE_NAMESPACE, PROCEDURE_NAME ) )
                .out( "id", Neo4jTypes.NTString ).out( "addresses", Neo4jTypes.NTList( Neo4jTypes.NTString ) )
                .out( "role", Neo4jTypes.NTString )
                .description( "Overview of all currently accessible cluster members and their roles." )
                .build() );
        this.topologyService = topologyService;
        this.leaderLocator = leaderLocator;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public RawIterator<Object[],ProcedureException> apply( Context ctx, Object[] input ) throws ProcedureException
    {
        List<ReadWriteEndPoint> endpoints = new ArrayList<>();
        CoreTopology coreTopology = topologyService.coreServers();
        Set<MemberId> coreMembers = coreTopology.members();
        MemberId leader = null;
        try
        {
            leader = leaderLocator.getLeader();
        }
        catch ( NoLeaderFoundException e )
        {
            log.debug( "No write server found. This can happen during a leader switch." );
        }

        for ( MemberId memberId : coreMembers )
        {
            Optional<ClientConnectorAddresses> clientConnectorAddresses =
                    coreTopology.find( memberId ).map( CoreServerInfo::connectors );
            if ( clientConnectorAddresses.isPresent() )
            {
                Role role = memberId.equals( leader ) ? Role.LEADER : Role.FOLLOWER;
                endpoints.add( new ReadWriteEndPoint( clientConnectorAddresses.get(), role, memberId.getUuid() ) );
            }
            else
            {
                log.debug( "No Address found for " + memberId );
            }
        }
        for ( ReadReplicaInfo readReplicaInfo : topologyService.readReplicas().allMemberInfo() )
        {
            endpoints.add( new ReadWriteEndPoint( readReplicaInfo.connectors(), Role.READ_REPLICA ) );
        }

        endpoints.sort( comparing( o -> o.addresses().toString() ) );

        return map( ( l ) -> new Object[]{l.identifier().toString(), l.addresses().uriList().stream().map( URI::toString ).toArray(), l.role().name()},
                asRawIterator( endpoints.iterator() ) );
    }

    private static class ReadWriteEndPoint
    {
        private static final UUID ZERO_ID = new UUID( 0, 0 );

        private final ClientConnectorAddresses clientConnectorAddresses;
        private final Role role;
        private final UUID identifier;

        public ClientConnectorAddresses addresses()
        {
            return clientConnectorAddresses;
        }

        public Role role()
        {
            return role;
        }

        UUID identifier()
        {
            return identifier == null ? ZERO_ID : identifier;
        }

        ReadWriteEndPoint( ClientConnectorAddresses clientConnectorAddresses, Role role )
        {
            this( clientConnectorAddresses, role, null );
        }

        ReadWriteEndPoint( ClientConnectorAddresses clientConnectorAddresses, Role role, UUID identifier )
        {
            this.clientConnectorAddresses = clientConnectorAddresses;
            this.role = role;
            this.identifier = identifier;
        }
    }
}
