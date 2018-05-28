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
package org.neo4j.causalclustering.discovery.procedures;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.Context;
import org.neo4j.kernel.api.proc.Neo4jTypes;
import org.neo4j.kernel.api.proc.QualifiedName;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.Comparator.comparing;
import static org.neo4j.helpers.collection.Iterables.asList;
import static org.neo4j.helpers.collection.Iterators.asRawIterator;
import static org.neo4j.helpers.collection.Iterators.map;
import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureSignature;

/**
 * Overview procedure with added support for server groups.
 */
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
                .out( "id", Neo4jTypes.NTString )
                .out( "addresses", Neo4jTypes.NTList( Neo4jTypes.NTString ) )
                .out( "role", Neo4jTypes.NTString )
                .out( "groups", Neo4jTypes.NTList( Neo4jTypes.NTString ) )
                .description( "Overview of all currently accessible cluster members and their roles." )
                .build() );
        this.topologyService = topologyService;
        this.leaderLocator = leaderLocator;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public RawIterator<Object[],ProcedureException> apply(
            Context ctx, Object[] input, ResourceTracker resourceTracker ) throws ProcedureException
    {
        List<ReadWriteEndPoint> endpoints = new ArrayList<>();
        CoreTopology coreTopology = topologyService.coreServers();
        Set<MemberId> coreMembers = coreTopology.members().keySet();
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
            Optional<CoreServerInfo> coreServerInfo = coreTopology.find( memberId );
            if ( coreServerInfo.isPresent() )
            {
                Role role = memberId.equals( leader ) ? Role.LEADER : Role.FOLLOWER;
                endpoints.add( new ReadWriteEndPoint( coreServerInfo.get().connectors(), role, memberId.getUuid(), asList( coreServerInfo.get().groups() ) ) );
            }
            else
            {
                log.debug( "No Address found for " + memberId );
            }
        }

        for ( Map.Entry<MemberId,ReadReplicaInfo> readReplica : topologyService.readReplicas().members().entrySet() )
        {
            ReadReplicaInfo readReplicaInfo = readReplica.getValue();
            endpoints.add( new ReadWriteEndPoint( readReplicaInfo.connectors(), Role.READ_REPLICA, readReplica.getKey().getUuid(), asList( readReplicaInfo.groups() ) ) );
        }

        endpoints.sort( comparing( o -> o.addresses().toString() ) );

        return map( ( endpoint ) -> new Object[]
                        {
                                endpoint.memberId().toString(),
                                endpoint.addresses().uriList().stream().map( URI::toString ).toArray(),
                                endpoint.role().name(),
                                endpoint.groups()
                        },
                asRawIterator( endpoints.iterator() ) );
    }

    static class ReadWriteEndPoint
    {
        private final ClientConnectorAddresses clientConnectorAddresses;
        private final Role role;
        private final UUID memberId;
        private final List<String> groups;

        public ClientConnectorAddresses addresses()
        {
            return clientConnectorAddresses;
        }

        public Role role()
        {
            return role;
        }

        UUID memberId()
        {
            return memberId;
        }

        List<String> groups()
        {
            return groups;
        }

        ReadWriteEndPoint( ClientConnectorAddresses clientConnectorAddresses, Role role, UUID memberId, List<String> groups )
        {
            this.clientConnectorAddresses = clientConnectorAddresses;
            this.role = role;
            this.memberId = memberId;
            this.groups = groups;
        }
    }
}
