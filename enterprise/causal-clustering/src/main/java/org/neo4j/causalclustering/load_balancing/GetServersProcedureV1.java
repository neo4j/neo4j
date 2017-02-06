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
package org.neo4j.causalclustering.load_balancing;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Stream;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.consensus.LeaderLocator;
import org.neo4j.causalclustering.core.consensus.NoLeaderFoundException;
import org.neo4j.causalclustering.discovery.CoreAddresses;
import org.neo4j.causalclustering.discovery.CoreTopologyService;
import org.neo4j.causalclustering.identity.MemberId;
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

import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.cluster_allow_reads_on_followers;
import static org.neo4j.causalclustering.load_balancing.Role.READ;
import static org.neo4j.causalclustering.load_balancing.Role.ROUTE;
import static org.neo4j.causalclustering.load_balancing.Role.WRITE;
import static org.neo4j.causalclustering.load_balancing.GetServersParameters.SERVERS;
import static org.neo4j.causalclustering.load_balancing.GetServersParameters.TTL;
import static org.neo4j.causalclustering.load_balancing.ProcedureNames.GET_SERVERS_V1;
import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureSignature;

/**
 * Returns endpoints and their capabilities.
 *
 * TODO: Detail signature (input and output).
 */
public class GetServersProcedureV1 implements CallableProcedure
{
    private final String DESCRIPTION = "Returns cluster endpoints and their capabilities.";

    private final ProcedureSignature procedureSignature =
            procedureSignature( GET_SERVERS_V1.fullyQualifiedProcedureName() )
                    .out( TTL.parameterName(), Neo4jTypes.NTInteger )
                    .out( SERVERS.parameterName(), Neo4jTypes.NTMap )
                    .description( DESCRIPTION )
                    .build();

    private final CoreTopologyService discoveryService;
    private final LeaderLocator leaderLocator;
    private final Config config;
    private final Log log;

    public GetServersProcedureV1( CoreTopologyService discoveryService, LeaderLocator leaderLocator,
            Config config, LogProvider logProvider )
    {
        this.discoveryService = discoveryService;
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
        List<EndPoint> routeEndpoints = routeEndpoints();
        List<EndPoint> writeEndpoints = writeEndpoints();
        List<EndPoint> readEndpoints = readEndpoints();
        return wrapUpEndpoints( routeEndpoints, writeEndpoints, readEndpoints );
    }

    private Optional<AdvertisedSocketAddress> leaderAdvertisedSocketAddress()
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

        return discoveryService.coreServers().find( leader )
                .map( server -> server.getClientConnectorAddresses().getBoltAddress() );
    }

    private List<EndPoint> routeEndpoints()
    {
        Stream<AdvertisedSocketAddress> routers =
                discoveryService.coreServers().addresses().stream()
                        .map( server -> server.getClientConnectorAddresses().getBoltAddress() );
        List<EndPoint> routeEndpoints = routers.map( EndPoint::route ).collect( toList() );
        Collections.shuffle( routeEndpoints );
        return routeEndpoints;
    }

    private List<EndPoint> writeEndpoints()
    {
        return leaderAdvertisedSocketAddress()
                .map( EndPoint::write ).map( Collections::singletonList ).orElse( emptyList() );
    }

    private List<EndPoint> readEndpoints()
    {
        List<AdvertisedSocketAddress> readReplicas = discoveryService.readReplicas().members().stream()
                .map( server -> server.getClientConnectorAddresses().getBoltAddress() ).collect( toList() );
        boolean addFollowers = readReplicas.isEmpty() || config.get( cluster_allow_reads_on_followers );
        Stream<AdvertisedSocketAddress> readCore = addFollowers ? coreReadEndPoints() : Stream.empty();
        List<EndPoint> readEndPoints =
                concat( readReplicas.stream(), readCore ).map( EndPoint::read ).collect( toList() );
        Collections.shuffle( readEndPoints );
        return readEndPoints;
    }

    private Stream<AdvertisedSocketAddress> coreReadEndPoints()
    {
        Optional<AdvertisedSocketAddress> leader = leaderAdvertisedSocketAddress();
        Collection<CoreAddresses> addresses = discoveryService.coreServers().addresses();
        Stream<AdvertisedSocketAddress> allAddresses = addresses.stream()
                .map( server -> server.getClientConnectorAddresses().getBoltAddress() );

        // if the leader is present and it is not alone filter it out from the read end points
        if ( leader.isPresent() && addresses.size() > 1 )
        {
            AdvertisedSocketAddress advertisedSocketAddress = leader.get();
            return allAddresses.filter( address -> !advertisedSocketAddress.equals( address ) );
        }

        // if there is only the leader return it as read end point
        // or if we cannot locate the leader return all cores as read end points
        return allAddresses;
    }

    private RawIterator<Object[],ProcedureException> wrapUpEndpoints( List<EndPoint> routeEndpoints,
            List<EndPoint> writeEndpoints, List<EndPoint> readEndpoints )
    {
        Object[] routers = routeEndpoints.stream().map( EndPoint::address ).toArray();
        Object[] readers = readEndpoints.stream().map( EndPoint::address ).toArray();
        Object[] writers = writeEndpoints.stream().map( EndPoint::address ).toArray();

        List<Map<String,Object>> servers = new ArrayList<>();

        if ( writers.length > 0 )
        {
            Map<String,Object> map = new TreeMap<>();

            map.put( "role", WRITE.name() );
            map.put( "addresses", writers );

            servers.add( map );
        }

        if ( readers.length > 0 )
        {
            Map<String,Object> map = new TreeMap<>();

            map.put( "role", READ.name() );
            map.put( "addresses", readers );

            servers.add( map );
        }

        if ( routers.length > 0 )
        {
            Map<String,Object> map = new TreeMap<>();

            map.put( "role", ROUTE.name() );
            map.put( "addresses", routers );

            servers.add( map );
        }

        long ttl = MILLISECONDS.toSeconds( config.get( CausalClusteringSettings.cluster_routing_ttl ) );
        Object[] row = new Object[]{ttl, servers};
        return RawIterator.<Object[],ProcedureException>of( row );
    }
}
