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

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.discovery.CoreServerInfo;
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

import static org.neo4j.causalclustering.routing.multi_cluster.procedure.ProcedureNames.GET_ROUTERS_FOR_ALL_DATABASES;
import static org.neo4j.causalclustering.routing.multi_cluster.procedure.ParameterNames.ROUTERS;
import static org.neo4j.causalclustering.routing.multi_cluster.procedure.ParameterNames.TTL;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureSignature;
import static org.neo4j.causalclustering.routing.Util.extractBoltAddress;

public class GetRoutersForAllDatabasesProcedure implements CallableProcedure
{

    private static final String DESCRIPTION = "Returns router capable endpoints for each database name in a multi-cluster.";

    private final ProcedureSignature procedureSignature =
            procedureSignature( GET_ROUTERS_FOR_ALL_DATABASES.fullyQualifiedProcedureName() )
                    .out( TTL.parameterName(), Neo4jTypes.NTInteger )
                    .out( ROUTERS.parameterName(), Neo4jTypes.NTList( Neo4jTypes.NTMap ) )
                    .description( DESCRIPTION )
                    .build();

    private final TopologyService topologyService;
    private final long timeToLiveMillis;

    public GetRoutersForAllDatabasesProcedure( TopologyService topologyService, Config config )
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
        Map<String,List<Endpoint>> routersPerDb = routeEndpoints();
        MultiClusterRoutingResult result = new MultiClusterRoutingResult( routersPerDb, timeToLiveMillis );
        return RawIterator.<Object[], ProcedureException>of( MultiClusterRoutingResultFormat.build( result ) );
    }

    private Map<String, List<Endpoint>> routeEndpoints()
    {
        Stream<CoreServerInfo> allCoreMemberInfo = topologyService.allCoreServers().members().values().stream();

        Map<String, List<CoreServerInfo>> coresByDb = allCoreMemberInfo.collect( Collectors.groupingBy( CoreServerInfo::getDatabaseName ) );

        Function<Map.Entry<String,List<CoreServerInfo>>, List<Endpoint>> extractQualifiedBoltAddresses = entry ->
        {
            List<CoreServerInfo> cores = entry.getValue();
            return cores.stream().map( extractBoltAddress() ).map( Endpoint::route ).collect( Collectors.toList() );
        };

        return coresByDb.entrySet().stream().collect( Collectors.toMap( Map.Entry::getKey, extractQualifiedBoltAddresses ) );
    }
}
