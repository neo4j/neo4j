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
package org.neo4j.coreedge.server.core;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.collection.RawIterator;
import org.neo4j.coreedge.discovery.ClusterTopology;
import org.neo4j.coreedge.discovery.ReadOnlyTopologyService;
import org.neo4j.coreedge.raft.LeaderLocator;
import org.neo4j.coreedge.raft.NoLeaderFoundException;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.BoltAddress;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.Neo4jTypes;
import org.neo4j.kernel.api.proc.ProcedureSignature;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.Integer.parseInt;
import static java.util.stream.Collectors.toSet;

import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.helpers.collection.Iterators.asRawIterator;
import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureSignature;

public class AcquireEndpointsProcedure extends CallableProcedure.BasicProcedure
{
    public static final String NAME = "acquireEndpoints";
    private final ReadOnlyTopologyService discoveryService;
    private final LeaderLocator<CoreMember> leaderLocator;
    private final Log log;

    public AcquireEndpointsProcedure( ReadOnlyTopologyService discoveryService,
                                      LeaderLocator<CoreMember> leaderLocator, LogProvider logProvider )
    {
        super( procedureSignature( new ProcedureSignature.ProcedureName( new String[]{"dbms", "cluster"}, NAME ) )
                .out( "address", Neo4jTypes.NTString ).out( "role", Neo4jTypes.NTString ).build());
        this.discoveryService = discoveryService;
        this.leaderLocator = leaderLocator;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public RawIterator<Object[], ProcedureException> apply( Context ctx, Object[] input ) throws ProcedureException
    {
        try
        {
            AdvertisedSocketAddress leader = leaderLocator.getLeader().getBoltAddress();
            Set<ReadWriteEndPoint> writeEndpoints = writeEndpoints( leader );
            Set<ReadWriteEndPoint> readEndpoints = readEndpoints( leader );

            log.info( "Write: %s, Read: %s",
                    writeEndpoints.stream().map( ReadWriteEndPoint::address ).collect( toSet() ),
                    readEndpoints.stream().map( ReadWriteEndPoint::address ).collect( toSet() ) );

            return wrapUpEndpoints( writeEndpoints, readEndpoints );
        }
        catch ( NoLeaderFoundException e )
        {
            throw new ProcedureException( Status.Cluster.NoLeader,
                    "No write server found. This can happen during a leader switch. " );
        }
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

    private Set<ReadWriteEndPoint> readEndpoints( AdvertisedSocketAddress leader ) throws NoLeaderFoundException
    {
        ClusterTopology clusterTopology = discoveryService.currentTopology();

        Stream<AdvertisedSocketAddress> readEdge = boltAddressesFor( clusterTopology.edgeMembers() );
        Stream<AdvertisedSocketAddress> readCore = boltAddressesFor( clusterTopology.boltCoreMembers() );
        Stream<AdvertisedSocketAddress> readLeader = Stream.of( leader );

        return Stream.concat(Stream.concat( readEdge, readCore ), readLeader).map( ReadWriteEndPoint::read )
                .limit( 1 ).collect( toSet() );
    }

    private Stream<AdvertisedSocketAddress> boltAddressesFor( Set<BoltAddress> boltAddresses )
    {
        return boltAddresses.stream().map( BoltAddress::getBoltAddress );
    }

    public enum Type
    {
        READ, WRITE
    }

    static class ReadWriteEndPoint
    {
        private final AdvertisedSocketAddress address;
        private final Type type;

        public String address()
        {
            return address.toString();
        }

        public String type()
        {
            return type.toString().toLowerCase();
        }

        public ReadWriteEndPoint( AdvertisedSocketAddress address, Type type )
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
