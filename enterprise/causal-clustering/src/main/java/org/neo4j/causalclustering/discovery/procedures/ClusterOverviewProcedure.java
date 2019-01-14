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
package org.neo4j.causalclustering.discovery.procedures;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.discovery.ClientConnectorAddresses;
import org.neo4j.causalclustering.discovery.CoreServerInfo;
import org.neo4j.causalclustering.discovery.CoreTopology;
import org.neo4j.causalclustering.discovery.ReadReplicaInfo;
import org.neo4j.causalclustering.discovery.RoleInfo;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.collection.RawIterator;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.Context;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.Collections.emptyMap;
import static java.util.Comparator.comparing;
import static org.neo4j.helpers.collection.Iterables.asList;
import static org.neo4j.helpers.collection.Iterators.asRawIterator;
import static org.neo4j.helpers.collection.Iterators.map;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureSignature;

/**
 * Overview procedure with added support for server groups.
 */
public class ClusterOverviewProcedure extends CallableProcedure.BasicProcedure
{
    private static final String[] PROCEDURE_NAMESPACE = {"dbms", "cluster"};
    public static final String PROCEDURE_NAME = "overview";
    private final TopologyService topologyService;
    private final Log log;

    public ClusterOverviewProcedure( TopologyService topologyService, LogProvider logProvider )
    {
        super( procedureSignature( new QualifiedName( PROCEDURE_NAMESPACE, PROCEDURE_NAME ) )
                .out( "id", Neo4jTypes.NTString )
                .out( "addresses", Neo4jTypes.NTList( Neo4jTypes.NTString ) )
                .out( "role", Neo4jTypes.NTString )
                .out( "groups", Neo4jTypes.NTList( Neo4jTypes.NTString ) )
                .out( "database", Neo4jTypes.NTString )
                .description( "Overview of all currently accessible cluster members and their roles." )
                .build() );
        this.topologyService = topologyService;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public RawIterator<Object[],ProcedureException> apply(
            Context ctx, Object[] input, ResourceTracker resourceTracker )
    {
        Map<MemberId,RoleInfo> roleMap = topologyService.allCoreRoles();
        List<ReadWriteEndPoint> endpoints = new ArrayList<>();

        CoreTopology coreTopology = topologyService.allCoreServers();
        Set<MemberId> coreMembers = coreTopology.members().keySet();

        for ( MemberId memberId : coreMembers )
        {
            Optional<CoreServerInfo> coreServerInfo = coreTopology.find( memberId );
            if ( coreServerInfo.isPresent() )
            {
                CoreServerInfo info = coreServerInfo.get();
                RoleInfo role = roleMap.getOrDefault( memberId, RoleInfo.UNKNOWN );
                endpoints.add( new ReadWriteEndPoint( info.connectors(), role, memberId.getUuid(),
                        asList( info.groups() ), info.getDatabaseName() ) );
            }
            else
            {
                log.debug( "No Address found for " + memberId );
            }
        }

        for ( Map.Entry<MemberId,ReadReplicaInfo> readReplica : topologyService.allReadReplicas().members().entrySet() )
        {
            ReadReplicaInfo readReplicaInfo = readReplica.getValue();
            endpoints.add( new ReadWriteEndPoint( readReplicaInfo.connectors(), RoleInfo.READ_REPLICA,
                    readReplica.getKey().getUuid(), asList( readReplicaInfo.groups() ), readReplicaInfo.getDatabaseName() ) );
        }

        endpoints.sort( comparing( o -> o.addresses().toString() ) );

        return map( endpoint -> new Object[]
                        {
                                endpoint.memberId().toString(),
                                endpoint.addresses().uriList().stream().map( URI::toString ).collect( Collectors.toList() ),
                                endpoint.role().name(),
                                endpoint.groups(),
                                endpoint.dbName()
                        },
                asRawIterator( endpoints.iterator() ) );
    }

    static class ReadWriteEndPoint
    {
        private final ClientConnectorAddresses clientConnectorAddresses;
        private final RoleInfo role;
        private final UUID memberId;
        private final List<String> groups;
        private final String dbName;

        public ClientConnectorAddresses addresses()
        {
            return clientConnectorAddresses;
        }

        public RoleInfo role()
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

        String dbName()
        {
            return dbName;
        }

        ReadWriteEndPoint( ClientConnectorAddresses clientConnectorAddresses, RoleInfo role, UUID memberId, List<String> groups, String dbName )
        {
            this.clientConnectorAddresses = clientConnectorAddresses;
            this.role = role;
            this.memberId = memberId;
            this.groups = groups;
            this.dbName = dbName;
        }
    }
}
