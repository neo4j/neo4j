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
package org.neo4j.causalclustering.load_balancing.procedure;

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
import org.neo4j.causalclustering.load_balancing.Endpoint;
import org.neo4j.causalclustering.load_balancing.LoadBalancingResult;
import org.neo4j.collection.RawIterator;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.Context;
import org.neo4j.kernel.api.proc.Neo4jTypes;
import org.neo4j.kernel.api.proc.ProcedureSignature;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.cluster_allow_reads_on_followers;
import static org.neo4j.causalclustering.load_balancing.Util.asList;
import static org.neo4j.causalclustering.load_balancing.Util.extractBoltAddress;
import static org.neo4j.causalclustering.load_balancing.procedure.ParameterNames.CONTEXT;
import static org.neo4j.causalclustering.load_balancing.procedure.ParameterNames.SERVERS;
import static org.neo4j.causalclustering.load_balancing.procedure.ParameterNames.TTL;
import static org.neo4j.causalclustering.load_balancing.procedure.ProcedureNames.GET_SERVERS_V2;

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
    public RawIterator<Object[],ProcedureException> apply( Context ctx, Object[] input ) throws ProcedureException
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

        return topologyService.coreServers().find( leader ).map( extractBoltAddress() );
    }

    private List<Endpoint> routeEndpoints()
    {
        Stream<AdvertisedSocketAddress> routers = topologyService.coreServers()
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
        List<AdvertisedSocketAddress> readReplicas = topologyService.readReplicas().allMemberInfo().stream()
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
        Collection<CoreServerInfo> coreServerInfo = topologyService.coreServers().members().values();
        Stream<AdvertisedSocketAddress> boltAddresses = topologyService.coreServers()
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
