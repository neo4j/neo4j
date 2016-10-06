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
package org.neo4j.coreedge.discovery.procedures;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.collection.RawIterator;
import org.neo4j.coreedge.core.CoreEdgeClusterSettings;
import org.neo4j.coreedge.core.consensus.LeaderLocator;
import org.neo4j.coreedge.core.consensus.NoLeaderFoundException;
import org.neo4j.coreedge.discovery.ClientConnectorAddresses;
import org.neo4j.coreedge.discovery.CoreTopologyService;
import org.neo4j.coreedge.discovery.EdgeAddresses;
import org.neo4j.coreedge.discovery.NoKnownAddressesException;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.Context;
import org.neo4j.kernel.api.proc.Neo4jTypes;
import org.neo4j.kernel.api.proc.QualifiedName;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.Collections.emptySet;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;
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
        Set<ReadWriteRouteEndPoint> writeEndpoints = emptySet();
        Set<ReadWriteRouteEndPoint> readEndpoints = readEndpoints();
        Set<ReadWriteRouteEndPoint> routeEndpoints = routeEndpoints();
        try
        {
            AdvertisedSocketAddress leaderAddress =
                    discoveryService.coreServers().find( leaderLocator.getLeader() )
                            .getClientConnectorAddresses().getBoltAddress();
            writeEndpoints = writeEndpoints( leaderAddress );
        }
        catch ( NoLeaderFoundException | NoKnownAddressesException e )
        {
            log.debug( "No write server found. This can happen during a leader switch." );
        }

        return wrapUpEndpoints( routeEndpoints, writeEndpoints, readEndpoints );
    }

    private Set<ReadWriteRouteEndPoint> routeEndpoints()
    {
        Stream<AdvertisedSocketAddress> routers =
                discoveryService.coreServers().addresses().stream()
                        .map( server -> server.getClientConnectorAddresses().getBoltAddress() );

        return routers.map( ReadWriteRouteEndPoint::route ).collect( toSet() );
    }

    private Set<ReadWriteRouteEndPoint> writeEndpoints( AdvertisedSocketAddress leader )
    {
        return Stream.of( leader ).map( ReadWriteRouteEndPoint::write ).collect( Collectors.toSet() );
    }

    private Set<ReadWriteRouteEndPoint> readEndpoints()
    {
        Stream<AdvertisedSocketAddress> readEdge =
                discoveryService.edgeServers().members().stream()
                        .map( EdgeAddresses::getClientConnectorAddresses )
                        .map( ClientConnectorAddresses::getBoltAddress );
        Stream<AdvertisedSocketAddress> readCore =
                discoveryService.coreServers().addresses().stream()
                        .map( server -> server.getClientConnectorAddresses().getBoltAddress() );

        return concat( readEdge, readCore ).map( ReadWriteRouteEndPoint::read ).collect( toSet() );
    }

    private RawIterator<Object[],ProcedureException> wrapUpEndpoints( Set<ReadWriteRouteEndPoint> routeEndpoints,
            Set<ReadWriteRouteEndPoint> writeEndpoints, Set<ReadWriteRouteEndPoint> readEndpoints )
    {
        Object[] routers = routeEndpoints.stream().map( ReadWriteRouteEndPoint::address ).sorted().toArray();
        Object[] readers = readEndpoints.stream().map( ReadWriteRouteEndPoint::address ).sorted().toArray();
        Object[] writers = writeEndpoints.stream().map( ReadWriteRouteEndPoint::address ).sorted().toArray();

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

        long ttl = MILLISECONDS.toSeconds( config.get( CoreEdgeClusterSettings.cluster_routing_ttl ) );
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
