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
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.neo4j.collection.RawIterator;
import org.neo4j.coreedge.discovery.ClusterTopology;
import org.neo4j.coreedge.discovery.CoreTopologyService;
import org.neo4j.coreedge.discovery.EdgeAddresses;
import org.neo4j.coreedge.discovery.NoKnownAddressesException;
import org.neo4j.coreedge.raft.LeaderLocator;
import org.neo4j.coreedge.raft.NoLeaderFoundException;
import org.neo4j.coreedge.messaging.AdvertisedSocketAddress;
import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.Neo4jTypes;
import org.neo4j.kernel.api.proc.ProcedureSignature;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.neo4j.helpers.collection.Iterators.asRawIterator;
import static org.neo4j.helpers.collection.Iterators.map;
import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureSignature;

public class ClusterOverviewProcedure extends CallableProcedure.BasicProcedure
{
    public static final String NAME = "overview";
    private final CoreTopologyService discoveryService;
    private final LeaderLocator leaderLocator;
    private final Log log;

    public ClusterOverviewProcedure( CoreTopologyService discoveryService,
                                     LeaderLocator leaderLocator, LogProvider logProvider )
    {
        super( procedureSignature( new ProcedureSignature.ProcedureName( new String[]{"dbms", "cluster"}, NAME ) )
                .out( "id", Neo4jTypes.NTString ).out( "address", Neo4jTypes.NTString )
                .out( "role", Neo4jTypes.NTString ).build() );
        this.discoveryService = discoveryService;
        this.leaderLocator = leaderLocator;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public RawIterator<Object[],ProcedureException> apply( Context ctx, Object[] input ) throws ProcedureException
    {
        List<ReadWriteEndPoint> endpoints = new ArrayList<>();
        ClusterTopology clusterTopology = discoveryService.currentTopology();
        Set<MemberId> coreMembers = clusterTopology.coreMembers();
        MemberId leader = null;
        try
        {
            leader = leaderLocator.getLeader();
        }
        catch ( NoLeaderFoundException e )
        {
            log.debug( "No write server found. This can happen during a leader switch." );
        }

        for ( MemberId memberId : coreMembers )
        {
            AdvertisedSocketAddress boltServerAddress = null;
            try
            {
                boltServerAddress = clusterTopology.coreAddresses( memberId ).getBoltServer();
            }
            catch ( NoKnownAddressesException e )
            {
                log.debug( "Address found for " );
            }
            Type type = memberId.equals( leader ) ? Type.LEADER : Type.FOLLOWER;
            endpoints.add( new ReadWriteEndPoint( boltServerAddress, type, memberId.getUuid() ) );
        }
        for ( EdgeAddresses edgeAddresses : clusterTopology.edgeMemberAddresses() )
        {
            endpoints.add( new ReadWriteEndPoint( edgeAddresses.getBoltAddress(), Type.READ_REPLICA, null ) );
        }

        Collections.sort( endpoints, ( o1, o2 ) -> o1.address().compareTo( o2.address() ) );

        return map( ( l ) -> new Object[]{l.identifier(), l.address(), l.type()},
                asRawIterator( endpoints.iterator() ) );
    }

    public enum Type
    {
        LEADER,
        FOLLOWER,
        READ_REPLICA
    }

    private static class ReadWriteEndPoint
    {
        private final AdvertisedSocketAddress address;
        private final Type type;
        private final UUID identifier;

        public String address()
        {
            return address == null ? null : address.toString();
        }

        public String type()
        {
            return type.toString().toLowerCase();
        }

        String identifier()
        {
            return identifier == null ? null : identifier.toString();
        }

        public ReadWriteEndPoint( AdvertisedSocketAddress address, Type type, UUID identifier )
        {
            this.address = address;
            this.type = type;
            this.identifier = identifier;
        }

    }
}
