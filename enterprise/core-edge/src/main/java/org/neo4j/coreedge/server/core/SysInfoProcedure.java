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
import java.util.UUID;
import java.util.stream.Stream;

import org.neo4j.collection.RawIterator;
import org.neo4j.coreedge.discovery.ClusterTopology;
import org.neo4j.coreedge.discovery.CoreAddresses;
import org.neo4j.coreedge.discovery.CoreTopologyService;
import org.neo4j.coreedge.raft.LeaderLocator;
import org.neo4j.coreedge.raft.NoLeaderFoundException;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.Neo4jTypes;
import org.neo4j.kernel.api.proc.ProcedureSignature;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.neo4j.coreedge.server.core.SysInfoProcedure.ReadWriteEndPoint.follower;
import static org.neo4j.coreedge.server.core.SysInfoProcedure.ReadWriteEndPoint.leader;
import static org.neo4j.helpers.collection.Iterators.asRawIterator;
import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureSignature;

class SysInfoProcedure extends CallableProcedure.BasicProcedure
{
    public static final String NAME = "overview";
    private final CoreTopologyService discoveryService;
    private final LeaderLocator leaderLocator;

    SysInfoProcedure(CoreTopologyService discoveryService,
                     LeaderLocator leaderLocator)
    {
        super( procedureSignature( new ProcedureSignature.ProcedureName( new String[]{"dbms", "cluster"}, NAME ) )
                .out( "id", Neo4jTypes.NTString )
                .out( "address", Neo4jTypes.NTString )
                .out( "role", Neo4jTypes.NTString ).build() );
        this.discoveryService = discoveryService;
        this.leaderLocator = leaderLocator;
    }

    @Override
    public RawIterator<Object[], ProcedureException> apply( Context ctx, Object[] input ) throws ProcedureException
    {
        try
        {
            CoreMember leader = leaderLocator.getLeader();
            ClusterTopology clusterTopology = discoveryService.currentTopology();

            Set<CoreMember> coreMembers = clusterTopology.coreMembers();

            Stream<ReadWriteEndPoint> leaderEndpoint = coreMembers.stream()
                    .filter( c -> c.equals( leader ) )
                    .map( c -> leader( clusterTopology.coreAddresses( c ).getBoltServer(), c.getUuid() ) );

            Stream<ReadWriteEndPoint> followerEndpoints = coreMembers.stream()
                    .filter( c -> !c.equals( leader ) )
                    .map( c -> follower( clusterTopology.coreAddresses( c ).getBoltServer(), c.getUuid() ) );

            Stream<ReadWriteEndPoint> readReplicaEndpoints = clusterTopology.edgeMembers().stream().map( m ->
                    ReadWriteEndPoint.readReplica( m.getBoltAddress() ) );

            Stream<ReadWriteEndPoint> allTheEndpoints = Stream.concat( leaderEndpoint,
                    Stream.concat( followerEndpoints, readReplicaEndpoints ) );

            return Iterators.map( ( l ) -> new Object[]{l.identifier(), l.address(), l.type()},
                    asRawIterator( allTheEndpoints.iterator() ) );

        }
        catch ( NoLeaderFoundException e )
        {
            throw new ProcedureException( Status.Cluster.NoLeader,
                    "No write server found. This can happen during a leader switch. " );
        }
    }

    public enum Type
    {
        LEADER, FOLLOWER, READ_REPLICA
    }

    static class ReadWriteEndPoint
    {
        private final AdvertisedSocketAddress address;
        private final Type type;
        private final String identifier;

        public String address()
        {
            return address.toString();
        }

        public String type()
        {
            return type.toString().toLowerCase();
        }

        String identifier()
        {
            return identifier ;
        }

        ReadWriteEndPoint(AdvertisedSocketAddress address, Type type, String identifier)
        {
            this.address = address;
            this.type = type;
            this.identifier = identifier;
        }

        public static ReadWriteEndPoint leader( AdvertisedSocketAddress address, UUID identifier )
        {
            return new ReadWriteEndPoint( address, Type.LEADER, identifier.toString() );
        }

        public static ReadWriteEndPoint follower( AdvertisedSocketAddress address, UUID identifier )
        {
            return new ReadWriteEndPoint( address, Type.FOLLOWER, identifier.toString() );
        }

        static ReadWriteEndPoint readReplica(AdvertisedSocketAddress address)
        {
            return new ReadWriteEndPoint( address, Type.READ_REPLICA, null );
        }
    }
}
