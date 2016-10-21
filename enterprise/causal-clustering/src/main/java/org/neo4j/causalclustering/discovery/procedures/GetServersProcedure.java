/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import org.neo4j.kernel.api.proc.QualifiedName;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.cluster_allow_reads_on_followers;
import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureSignature;

/*
C: RUN "CALL dbms.cluster.routing.getServers" {}
   PULL_ALL
S: SUCCESS {"fields": ["ttl", "servers"]}
   RECORD [9223372036854775807, [
{"role": "WRITE", "addresses": ["127.0.0.1:9001"]},
{"role": "READ", "addresses": ["127.0.0.1:9002", "127.0.0.1:9003"]},
{"role": "ROUTE", "addresses": ["127.0.0.1:9001", "127.0.0.1:9002", "127.0.0.1:9003"]}
]]
 */
public class GetServersProcedure extends CallableProcedure.BasicProcedure
{
    public static final String NAME = "getServers";
    private final CoreTopologyService discoveryService;
    private final LeaderLocator leaderLocator;
    private final Config config;
    private final Log log;

    public GetServersProcedure( CoreTopologyService discoveryService, LeaderLocator leaderLocator, Config config,
            LogProvider logProvider )
    {
        super( procedureSignature( new QualifiedName( new String[]{"dbms", "cluster", "routing"}, NAME ) )
                .out( "ttl", Neo4jTypes.NTInteger ).out( "servers", Neo4jTypes.NTMap )
                .description( "Provides recommendations about servers that support reads, writes, and can act as " +
                        "routers." )
                .build() );

        this.discoveryService = discoveryService;
        this.leaderLocator = leaderLocator;
        this.config = config;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public RawIterator<Object[],ProcedureException> apply( Context ctx, Object[] input ) throws ProcedureException
    {
        List<ReadWriteRouteEndPoint> writeEndpoints = writeEndpoints();
        List<ReadWriteRouteEndPoint> readEndpoints = readEndpoints();
        List<ReadWriteRouteEndPoint> routeEndpoints = routeEndpoints();
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

    private List<ReadWriteRouteEndPoint> routeEndpoints()
    {
        Stream<AdvertisedSocketAddress> routers =
                discoveryService.coreServers().addresses().stream()
                        .map( server -> server.getClientConnectorAddresses().getBoltAddress() );
        List<ReadWriteRouteEndPoint> routeEndpoints = routers.map( ReadWriteRouteEndPoint::route ).collect( toList() );
        Collections.shuffle( routeEndpoints );
        return routeEndpoints;
    }

    private List<ReadWriteRouteEndPoint> writeEndpoints()
    {
        return leaderAdvertisedSocketAddress()
                .map( ReadWriteRouteEndPoint::write ).map( Collections::singletonList ).orElse( emptyList() );
    }

    private List<ReadWriteRouteEndPoint> readEndpoints()
    {
        List<AdvertisedSocketAddress> readReplicas = discoveryService.readReplicas().members().stream()
                .map( server -> server.getClientConnectorAddresses().getBoltAddress() ).collect( toList() );
        boolean addFollowers = readReplicas.isEmpty() || config.get( cluster_allow_reads_on_followers );
        Stream<AdvertisedSocketAddress> readCore = addFollowers ? coreReadEndPoints() : Stream.empty();
        List<ReadWriteRouteEndPoint> readEndPoints =
                concat( readReplicas.stream(), readCore ).map( ReadWriteRouteEndPoint::read ).collect( toList() );
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

    private RawIterator<Object[],ProcedureException> wrapUpEndpoints( List<ReadWriteRouteEndPoint> routeEndpoints,
            List<ReadWriteRouteEndPoint> writeEndpoints, List<ReadWriteRouteEndPoint> readEndpoints )
    {
        Object[] routers = routeEndpoints.stream().map( ReadWriteRouteEndPoint::address ).toArray();
        Object[] readers = readEndpoints.stream().map( ReadWriteRouteEndPoint::address ).toArray();
        Object[] writers = writeEndpoints.stream().map( ReadWriteRouteEndPoint::address ).toArray();

        List<Map<String,Object>> servers = new ArrayList<>();

        if ( writers.length > 0 )
        {
            Map<String,Object> map = new TreeMap<>();

            map.put( "role",  Type.WRITE.name());
            map.put( "addresses", writers );

            servers.add( map );
        }

        if ( readers.length > 0 )
        {
            Map<String,Object> map = new TreeMap<>();

            map.put( "role",  Type.READ.name());
            map.put( "addresses", readers);

            servers.add( map );

        }

        if ( routers.length > 0 )
        {
            Map<String,Object> map = new TreeMap<>();

            map.put( "role",  Type.ROUTE.name());
            map.put( "addresses", routers);

            servers.add( map );
        }

        long ttl = MILLISECONDS.toSeconds( config.get( CausalClusteringSettings.cluster_routing_ttl ) );
        Object[] row = new Object[] {ttl, servers };
        return RawIterator.<Object[], ProcedureException>of(row);
    }

    public enum Type
    {
        READ,
        WRITE,
        ROUTE
    }

    private static class ReadWriteRouteEndPoint
    {
        private final AdvertisedSocketAddress address;
        private final Type type;

        public String address()
        {
            return address.toString();
        }

        public String type()
        {
            return type.toString().toUpperCase();
        }

        ReadWriteRouteEndPoint( AdvertisedSocketAddress address, Type type )
        {
            this.address = address;
            this.type = type;
        }

        public static ReadWriteRouteEndPoint write( AdvertisedSocketAddress address )
        {
            return new ReadWriteRouteEndPoint( address, Type.WRITE );
        }

        public static ReadWriteRouteEndPoint read( AdvertisedSocketAddress address )
        {
            return new ReadWriteRouteEndPoint( address, Type.READ );
        }

        static ReadWriteRouteEndPoint route( AdvertisedSocketAddress address )
        {
            return new ReadWriteRouteEndPoint( address, Type.ROUTE );
        }

        @Override
        public String toString()
        {
            return "ReadWriteRouteEndPoint{" + "address=" + address + ", type=" + type + '}';
        }
    }
}
