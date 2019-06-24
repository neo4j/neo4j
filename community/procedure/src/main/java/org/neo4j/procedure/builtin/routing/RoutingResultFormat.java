/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.procedure.builtin.routing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.VirtualValues;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.procedure.builtin.routing.Role.READ;
import static org.neo4j.procedure.builtin.routing.Role.ROUTE;
import static org.neo4j.procedure.builtin.routing.Role.WRITE;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;

/**
 * The result format of GetServersV1 and GetServersV2 procedures.
 */
public class RoutingResultFormat
{
    private static final String ROLE_KEY = "role";
    private static final String ADDRESSES_KEY = "addresses";

    private RoutingResultFormat()
    {
    }

    public static AnyValue[] build( RoutingResult result )
    {
        AnyValue[] routers = asValues( result.routeEndpoints() );
        AnyValue[] readers = asValues( result.readEndpoints() );
        AnyValue[] writers = asValues( result.writeEndpoints() );

        List<AnyValue> servers = new ArrayList<>();

        if ( writers.length > 0 )
        {
            MapValueBuilder builder = new MapValueBuilder();

            builder.add( ROLE_KEY, stringValue( WRITE.name() ) );
            builder.add( ADDRESSES_KEY, VirtualValues.list( writers ) );

            servers.add( builder.build() );
        }

        if ( readers.length > 0 )
        {
            MapValueBuilder builder = new MapValueBuilder();

            builder.add( ROLE_KEY, stringValue( READ.name() ) );
            builder.add( ADDRESSES_KEY, VirtualValues.list( readers ) );

            servers.add( builder.build() );
        }

        if ( routers.length > 0 )
        {
            MapValueBuilder builder = new MapValueBuilder();

            builder.add( ROLE_KEY, stringValue( ROUTE.name() ) );
            builder.add( ADDRESSES_KEY, VirtualValues.list( routers ) );

            servers.add( builder.build() );
        }

        LongValue timeToLiveSeconds = longValue( MILLISECONDS.toSeconds( result.ttlMillis() ) );
        return new AnyValue[]{timeToLiveSeconds, VirtualValues.fromList( servers )};
    }

    public static RoutingResult parse( AnyValue[] record )
    {
        LongValue timeToLiveSeconds = (LongValue) record[0];
        ListValue endpointData = (ListValue) record[1];

        Map<Role,List<SocketAddress>> endpoints = parseRows( endpointData );

        return new RoutingResult(
                endpoints.get( ROUTE ),
                endpoints.get( WRITE ),
                endpoints.get( READ ),
                timeToLiveSeconds.longValue() * 1000 );
    }

    public static RoutingResult parse( MapValue record )
    {
        return parse( new AnyValue[]{
                record.get( ParameterNames.TTL.parameterName() ),
                record.get( ParameterNames.SERVERS.parameterName() )
        } );
    }

    public static List<SocketAddress> parseEndpoints( ListValue addresses )
    {
        List<SocketAddress> result = new ArrayList<>( addresses.size() );
        for ( AnyValue address : addresses )
        {
            result.add( parseAddress( ((TextValue) address).stringValue() ) );
        }
        return result;
    }

    private static Map<Role,List<SocketAddress>> parseRows( ListValue rows )
    {
        Map<Role,List<SocketAddress>> endpoints = new HashMap<>();
        for ( AnyValue single : rows )
        {
            MapValue row = (MapValue) single;
            Role role = Role.valueOf( ((TextValue) row.get( "role" )).stringValue() );
            List<SocketAddress> addresses = parseEndpoints( (ListValue) row.get( "addresses" ) );
            endpoints.put( role, addresses );
        }

        Arrays.stream( Role.values() ).forEach( r -> endpoints.putIfAbsent( r, Collections.emptyList() ) );

        return endpoints;
    }

    private static SocketAddress parseAddress( String address )
    {
        String[] split = address.split( ":" );
        return new SocketAddress( split[0], Integer.valueOf( split[1] ) );
    }

    private static AnyValue[] asValues( List<SocketAddress> addresses )
    {
        return addresses.stream()
                .map( SocketAddress::toString )
                .map( Values::stringValue )
                .toArray( AnyValue[]::new );
    }
}
