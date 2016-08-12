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
import org.neo4j.coreedge.discovery.ClusterTopology;
import org.neo4j.coreedge.discovery.CoreAddresses;
import org.neo4j.coreedge.discovery.CoreTopologyService;
import org.neo4j.coreedge.discovery.EdgeAddresses;
import org.neo4j.coreedge.discovery.NoKnownAddressesException;
import org.neo4j.coreedge.core.consensus.LeaderLocator;
import org.neo4j.coreedge.core.consensus.NoLeaderFoundException;
import org.neo4j.coreedge.messaging.address.AdvertisedSocketAddress;
import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.Neo4jTypes;
import org.neo4j.kernel.api.proc.ProcedureSignature;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toSet;
import static org.neo4j.helpers.collection.Iterators.asRawIterator;
import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureSignature;

public class AcquireEndpointsProcedure extends CallableProcedure.BasicProcedure
{
    public static final String NAME = "acquireEndpoints";
    private final CoreTopologyService discoveryService;
    private final LeaderLocator leaderLocator;
    private final Log log;

    public AcquireEndpointsProcedure( CoreTopologyService discoveryService,
                                      LeaderLocator leaderLocator, LogProvider logProvider )
    {
        super( procedureSignature( new ProcedureSignature.ProcedureName( new String[]{"dbms", "cluster"}, NAME ) )
                .out( "address", Neo4jTypes.NTString ).out( "role", Neo4jTypes.NTString ).build() );
        this.discoveryService = discoveryService;
        this.leaderLocator = leaderLocator;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public RawIterator<Object[], ProcedureException> apply( Context ctx, Object[] input ) throws ProcedureException
    {
        Set<ReadWriteEndPoint> writeEndpoints = emptySet();
        try
        {
            MemberId leader = leaderLocator.getLeader();
            AdvertisedSocketAddress leaderAddress =
                    discoveryService.currentTopology().coreAddresses( leader ).getBoltServer();
            writeEndpoints = writeEndpoints( leaderAddress );
        }
        catch ( NoLeaderFoundException | NoKnownAddressesException e )
        {
            log.debug( "No write server found. This can happen during a leader switch." );
        }
        Set<ReadWriteEndPoint> readEndpoints = readEndpoints();

        log.info( "Write: %s, Read: %s",
                writeEndpoints.stream().map( ReadWriteEndPoint::address ).collect( toSet() ),
                readEndpoints.stream().map( ReadWriteEndPoint::address ).collect( toSet() ) );

        return wrapUpEndpoints( writeEndpoints, readEndpoints );
    }

    private Set<ReadWriteEndPoint> writeEndpoints( AdvertisedSocketAddress leader )
    {
        return Stream.of( leader ).map( ReadWriteEndPoint::write ).collect( Collectors.toSet() );
    }

    private RawIterator<Object[], ProcedureException> wrapUpEndpoints( Set<ReadWriteEndPoint> writeEndpoints,
                                                                       Set<ReadWriteEndPoint> readEndpoints )
    {
        return Iterators.map( ( l ) -> new Object[]{l.address(), l.type()},
                asRawIterator( Stream.concat( writeEndpoints.stream(), readEndpoints.stream() ).iterator() ) );
    }

    private Set<ReadWriteEndPoint> readEndpoints()
    {
        ClusterTopology clusterTopology = discoveryService.currentTopology();

        Stream<AdvertisedSocketAddress> readEdge = clusterTopology.edgeMemberAddresses().stream()
                .map( EdgeAddresses::getBoltAddress );
        Stream<AdvertisedSocketAddress> readCore = clusterTopology.coreMemberAddresses().stream()
                .map( CoreAddresses::getBoltServer );

        return Stream.concat( readEdge, readCore ).map( ReadWriteEndPoint::read )
                .limit( 1 ).collect( toSet() );
    }

    public enum Type
    {
        READ, WRITE
    }

    private static class ReadWriteEndPoint
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

        ReadWriteEndPoint( AdvertisedSocketAddress address, Type type )
        {
            this.address = address;
            this.type = type;
        }

        public static ReadWriteEndPoint write( AdvertisedSocketAddress address )
        {
            return new ReadWriteEndPoint( address, Type.WRITE );
        }

        public static ReadWriteEndPoint read( AdvertisedSocketAddress address )
        {
            return new ReadWriteEndPoint( address, Type.READ );
        }
    }
}
