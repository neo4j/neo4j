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
package org.neo4j.causalclustering.routing.load_balancing.procedure;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.consensus.LeaderLocator;
import org.neo4j.causalclustering.core.consensus.NoLeaderFoundException;
import org.neo4j.causalclustering.discovery.CoreServerInfo;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.routing.Endpoint;
import org.neo4j.causalclustering.routing.load_balancing.LoadBalancingResult;
import org.neo4j.collection.RawIterator;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.Context;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.procedure.Mode;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.cluster_allow_reads_on_followers;
import static org.neo4j.causalclustering.routing.Util.asList;
import static org.neo4j.causalclustering.routing.Util.extractBoltAddress;
import static org.neo4j.causalclustering.routing.load_balancing.procedure.ParameterNames.CONTEXT;
import static org.neo4j.causalclustering.routing.load_balancing.procedure.ParameterNames.SERVERS;
import static org.neo4j.causalclustering.routing.load_balancing.procedure.ParameterNames.TTL;
import static org.neo4j.causalclustering.routing.load_balancing.procedure.ProcedureNames.GET_SERVERS_V2;

/**
 * Returns endpoints and their capabilities.
 *
 * GetServersV2 extends upon V1 by allowing a client context consisting of
 * key-value pairs to be supplied to and used by the concrete load
 * balancing strategies.
 */
public class GetServersProcedureForSingleDC implements CallableProcedure
{
    private final String DESCRIPTION = "Returns cluster endpoints and their capabilities for single data center setup.";

    private final ProcedureSignature procedureSignature =
            ProcedureSignature.procedureSignature( GET_SERVERS_V2.fullyQualifiedProcedureName() )
                    .in( CONTEXT.parameterName(), Neo4jTypes.NTMap )
                    .out( TTL.parameterName(), Neo4jTypes.NTInteger )
                    .out( SERVERS.parameterName(), Neo4jTypes.NTList( Neo4jTypes.NTMap ) )
                    .mode( Mode.DBMS )
                    .description( DESCRIPTION )
                    .build();

    private final TopologyService topologyService;
    private final LeaderLocator leaderLocator;
    private final Config config;
    private final Log log;

    public GetServersProcedureForSingleDC( TopologyService topologyService, LeaderLocator leaderLocator,
            Config config, LogProvider logProvider )
    {
        this.topologyService = topologyService;
        this.leaderLocator = leaderLocator;
        this.config = config;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public ProcedureSignature signature()
    {
        return procedureSignature;
    }

    @Override
    public RawIterator<Object[],ProcedureException> apply(
            Context ctx, Object[] input, ResourceTracker resourceTracker )
    {
        List<Endpoint> routeEndpoints = routeEndpoints();
        List<Endpoint> writeEndpoints = writeEndpoints();
        List<Endpoint> readEndpoints = readEndpoints();

        return RawIterator.<Object[],ProcedureException>of( ResultFormatV1.build(
                new LoadBalancingResult( routeEndpoints, writeEndpoints, readEndpoints,
                        config.get( CausalClusteringSettings.cluster_routing_ttl ).toMillis() ) ) );
    }

    private Optional<AdvertisedSocketAddress> leaderBoltAddress()
    {
        MemberId leader;
        try
        {
            leader = leaderLocator.getLeader();
        }
        catch ( NoLeaderFoundException e )
        {
            log.debug( "No leader server found. This can happen during a leader switch. No write end points available" );
            return Optional.empty();
        }

        return topologyService.localCoreServers().find( leader ).map( extractBoltAddress() );
    }

    private List<Endpoint> routeEndpoints()
    {
        Stream<AdvertisedSocketAddress> routers = topologyService.localCoreServers()
                .members().values().stream().map( extractBoltAddress() );
        List<Endpoint> routeEndpoints = routers.map( Endpoint::route ).collect( toList() );
        Collections.shuffle( routeEndpoints );
        return routeEndpoints;
    }

    private List<Endpoint> writeEndpoints()
    {
        return asList( leaderBoltAddress().map( Endpoint::write ) );
    }

    private List<Endpoint> readEndpoints()
    {
        List<AdvertisedSocketAddress> readReplicas = topologyService.localReadReplicas().allMemberInfo().stream()
                .map( extractBoltAddress() ).collect( toList() );
        boolean addFollowers = readReplicas.isEmpty() || config.get( cluster_allow_reads_on_followers );
        Stream<AdvertisedSocketAddress> readCore = addFollowers ? coreReadEndPoints() : Stream.empty();
        List<Endpoint> readEndPoints =
                concat( readReplicas.stream(), readCore ).map( Endpoint::read ).collect( toList() );
        Collections.shuffle( readEndPoints );
        return readEndPoints;
    }

    private Stream<AdvertisedSocketAddress> coreReadEndPoints()
    {
        Optional<AdvertisedSocketAddress> leader = leaderBoltAddress();
        Collection<CoreServerInfo> coreServerInfo = topologyService.localCoreServers().members().values();
        Stream<AdvertisedSocketAddress> boltAddresses = topologyService.localCoreServers()
                .members().values().stream().map( extractBoltAddress() );

        // if the leader is present and it is not alone filter it out from the read end points
        if ( leader.isPresent() && coreServerInfo.size() > 1 )
        {
            AdvertisedSocketAddress advertisedSocketAddress = leader.get();
            return boltAddresses.filter( address -> !advertisedSocketAddress.equals( address ) );
        }

        // if there is only the leader return it as read end point
        // or if we cannot locate the leader return all cores as read end points
        return boltAddresses;
    }
}
