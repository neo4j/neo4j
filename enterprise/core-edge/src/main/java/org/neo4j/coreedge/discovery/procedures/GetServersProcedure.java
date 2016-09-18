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

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.collection.RawIterator;
import org.neo4j.coreedge.core.consensus.LeaderLocator;
import org.neo4j.coreedge.core.consensus.NoLeaderFoundException;
import org.neo4j.coreedge.discovery.CoreAddresses;
import org.neo4j.coreedge.discovery.CoreTopologyService;
import org.neo4j.coreedge.discovery.EdgeAddresses;
import org.neo4j.coreedge.discovery.NoKnownAddressesException;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.Context;
import org.neo4j.kernel.api.proc.Neo4jTypes;
import org.neo4j.kernel.api.proc.QualifiedName;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;

import static org.neo4j.helpers.collection.Iterators.asRawIterator;
import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureSignature;

public class GetServersProcedure extends CallableProcedure.BasicProcedure
{
    public static final String NAME = "getServers";
    private final CoreTopologyService discoveryService;
    private final LeaderLocator leaderLocator;
    private final Log log;

    public GetServersProcedure( CoreTopologyService discoveryService, LeaderLocator leaderLocator, LogProvider
            logProvider )
    {
        super( procedureSignature( new QualifiedName( new String[]{"dbms", "cluster", "routing"}, NAME ) )
                .out( "address", Neo4jTypes.NTString )
                .out( "role", Neo4jTypes.NTString )
                .out( "expiry", Neo4jTypes.NTInteger)
                .build() );

        this.discoveryService = discoveryService;
        this.leaderLocator = leaderLocator;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public RawIterator<Object[], ProcedureException> apply( Context ctx, Object[] input ) throws ProcedureException
    {
        Set<ReadWriteRouteEndPoint> writeEndpoints = emptySet();
        Set<ReadWriteRouteEndPoint> readEndpoints = emptySet();
        Set<ReadWriteRouteEndPoint> routeEndpoints = emptySet();
        try
        {
            readEndpoints = readEndpoints();

            AdvertisedSocketAddress leaderAddress =
                    discoveryService.coreServers().find( leaderLocator.getLeader() ).getBoltServer();
            writeEndpoints = writeEndpoints( leaderAddress );

            routeEndpoints = routeEndpoints();

        }
        catch ( NoLeaderFoundException | NoKnownAddressesException e )
        {
            log.debug( "No write server found. This can happen during a leader switch." );
        }

        return wrapUpEndpoints( routeEndpoints, writeEndpoints, readEndpoints );
    }

    private Set<ReadWriteRouteEndPoint> routeEndpoints()
    {
        Stream<AdvertisedSocketAddress> readCore = discoveryService.coreServers().addresses().stream()
                .map( CoreAddresses::getBoltServer );

        return readCore.map( ReadWriteRouteEndPoint::route ).collect( toSet() );
    }

    private Set<ReadWriteRouteEndPoint> writeEndpoints( AdvertisedSocketAddress leader )
    {
        return Stream.of( leader ).map( ReadWriteRouteEndPoint::write ).collect( Collectors.toSet() );
    }

    private Set<ReadWriteRouteEndPoint> readEndpoints()
    {
        Stream<AdvertisedSocketAddress> readEdge = discoveryService.edgeServers().members().stream()
                .map( EdgeAddresses::getBoltAddress );
        Stream<AdvertisedSocketAddress> readCore = discoveryService.coreServers().addresses().stream()
                .map( CoreAddresses::getBoltServer );

        return concat( readEdge, readCore ).map( ReadWriteRouteEndPoint::read ).collect( toSet() );
    }

    private RawIterator<Object[], ProcedureException> wrapUpEndpoints( Set<ReadWriteRouteEndPoint> routeEndpoints,
                                                                       Set<ReadWriteRouteEndPoint> writeEndpoints,
                                                                       Set<ReadWriteRouteEndPoint> readEndpoints )
    {
        return Iterators.map( ( readWriteRouteEndPoint ) -> new Object[]{readWriteRouteEndPoint.address(),
                        readWriteRouteEndPoint.type(), readWriteRouteEndPoint.expiry()},
                asRawIterator( concat( routeEndpoints.stream(),
                        concat( writeEndpoints.stream(), readEndpoints.stream() ) ).iterator() ) );
    }

    public enum Type
    {
        READ, WRITE, ROUTE
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

        long expiry()
        {
            return Long.MAX_VALUE;
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
    }
}
