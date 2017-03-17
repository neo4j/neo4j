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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

import org.neo4j.causalclustering.load_balancing.Endpoint;
import org.neo4j.causalclustering.load_balancing.LoadBalancingProcessor;
import org.neo4j.causalclustering.load_balancing.LoadBalancingResult;
import org.neo4j.causalclustering.load_balancing.Role;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.SocketAddress;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static org.neo4j.causalclustering.load_balancing.Role.READ;
import static org.neo4j.causalclustering.load_balancing.Role.ROUTE;
import static org.neo4j.causalclustering.load_balancing.Role.WRITE;

/**
 * The result format of GetServersV1 and GetServersV2 procedures.
 */
public class ResultFormatV1
{
    private static final String ROLE_KEY = "role";
    private static final String ADDRESSES_KEY = "addresses";

    static Object[] build( LoadBalancingProcessor.Result result )
    {
        Object[] routers = result.routeEndpoints().stream().map( Endpoint::address ).map( SocketAddress::toString ).toArray();
        Object[] readers = result.readEndpoints().stream().map( Endpoint::address ).map( SocketAddress::toString ).toArray();
        Object[] writers = result.writeEndpoints().stream().map( Endpoint::address ).map( SocketAddress::toString ).toArray();

        List<Map<String,Object>> servers = new ArrayList<>();

        if ( writers.length > 0 )
        {
            Map<String,Object> map = new TreeMap<>();

            map.put( ROLE_KEY, WRITE.name() );
            map.put( ADDRESSES_KEY, writers );

            servers.add( map );
        }

        if ( readers.length > 0 )
        {
            Map<String,Object> map = new TreeMap<>();

            map.put( ROLE_KEY, READ.name() );
            map.put( ADDRESSES_KEY, readers );

            servers.add( map );
        }

        if ( routers.length > 0 )
        {
            Map<String,Object> map = new TreeMap<>();

            map.put( ROLE_KEY, ROUTE.name() );
            map.put( ADDRESSES_KEY, routers );

            servers.add( map );
        }

        long timeToLiveSeconds = MILLISECONDS.toSeconds( result.getTimeToLiveMillis() );
        return new Object[]{timeToLiveSeconds, servers};
    }

    public static LoadBalancingResult parse( Object[] record )
    {
        long timeToLiveSeconds = (long) record[0];
        @SuppressWarnings( "unchecked" )
        List<Map<String,Object>> endpointData = (List<Map<String,Object>>) record[1];

        Map<Role,List<Endpoint>> endpoints = parse( endpointData );

        return new LoadBalancingResult(
                endpoints.get( ROUTE ),
                endpoints.get( WRITE ),
                endpoints.get( READ ),
                timeToLiveSeconds * 1000 );
    }

    public static LoadBalancingResult parse( Map<String,Object> record )
    {
        return parse( new Object[]{
                record.get( ParameterNames.TTL.parameterName() ),
                record.get( ParameterNames.SERVERS.parameterName() )
        } );
    }

    private static Map<Role,List<Endpoint>> parse( List<Map<String,Object>> result )
    {
        Map<Role,List<Endpoint>> endpoints = new HashMap<>();
        for ( Map<String,Object> single : result )
        {
            Role role = Role.valueOf( (String) single.get( "role" ) );
            List<Endpoint> addresses = parse( (Object[]) single.get( "addresses" ), role );
            endpoints.put( role, addresses );
        }

        Arrays.stream( Role.values() ).forEach( r -> endpoints.putIfAbsent( r, Collections.emptyList() ) );

        return endpoints;
    }

    private static List<Endpoint> parse( Object[] addresses, Role role )
    {
        return Stream.of( addresses )
                .map( rawAddress -> parse( (String) rawAddress ) )
                .map( address -> new Endpoint( address, role ) )
                .collect( toList() );
    }

    private static AdvertisedSocketAddress parse( String address )
    {
        String[] split = address.split( ":" );
        return new AdvertisedSocketAddress( split[0], Integer.valueOf( split[1] ) );
    }
}
