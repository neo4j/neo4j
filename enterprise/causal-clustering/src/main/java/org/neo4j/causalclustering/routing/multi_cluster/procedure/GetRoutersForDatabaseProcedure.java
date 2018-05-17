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
package org.neo4j.causalclustering.routing.multi_cluster.procedure;

import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.discovery.CoreServerInfo;
import org.neo4j.causalclustering.discovery.CoreTopology;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.routing.Endpoint;
import org.neo4j.causalclustering.routing.multi_cluster.MultiClusterRoutingResult;
import org.neo4j.collection.RawIterator;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.Context;
import org.neo4j.kernel.configuration.Config;

import static org.neo4j.causalclustering.routing.multi_cluster.procedure.ProcedureNames.GET_ROUTERS_FOR_DATABASE;
import static org.neo4j.causalclustering.routing.multi_cluster.procedure.ParameterNames.DATABASE;
import static org.neo4j.causalclustering.routing.multi_cluster.procedure.ParameterNames.ROUTERS;
import static org.neo4j.causalclustering.routing.multi_cluster.procedure.ParameterNames.TTL;
import static org.neo4j.causalclustering.routing.Util.extractBoltAddress;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureSignature;

public class GetRoutersForDatabaseProcedure implements CallableProcedure
{
    private static final String DESCRIPTION = "Returns router capable endpoints for a specific database in a multi-cluster.";

    private final ProcedureSignature procedureSignature =
            procedureSignature( GET_ROUTERS_FOR_DATABASE.fullyQualifiedProcedureName() )
                    .in( DATABASE.parameterName(), Neo4jTypes.NTString )
                    .out( TTL.parameterName(), Neo4jTypes.NTInteger )
                    .out( ROUTERS.parameterName(), Neo4jTypes.NTList( Neo4jTypes.NTMap ) )
                    .description( DESCRIPTION )
                    .build();

    private final TopologyService topologyService;
    private final long timeToLiveMillis;

    public GetRoutersForDatabaseProcedure( TopologyService topologyService, Config config )
    {
        this.topologyService = topologyService;
        this.timeToLiveMillis = config.get( CausalClusteringSettings.cluster_routing_ttl ).toMillis();
    }

    @Override
    public ProcedureSignature signature()
    {
        return procedureSignature;
    }

    @Override
    public RawIterator<Object[],ProcedureException> apply( Context ctx, Object[] input, ResourceTracker resourceTracker ) throws ProcedureException
    {
        @SuppressWarnings( "unchecked" )
        String dbName = (String) input[0];
        List<Endpoint> routers = routeEndpoints( dbName );

        HashMap<String,List<Endpoint>> routerMap = new HashMap<>();
        routerMap.put( dbName, routers );

        MultiClusterRoutingResult result = new MultiClusterRoutingResult( routerMap, timeToLiveMillis );
        return RawIterator.<Object[], ProcedureException>of( MultiClusterRoutingResultFormat.build( result ) );
    }

    private List<Endpoint> routeEndpoints( String dbName )
    {
        CoreTopology filtered = topologyService.allCoreServers().filterTopologyByDb( dbName );
        Stream<CoreServerInfo> filteredCoreMemberInfo = filtered.members().values().stream();

        return filteredCoreMemberInfo.map( extractBoltAddress() ).map( Endpoint::route ).collect( Collectors.toList() );
    }
}
