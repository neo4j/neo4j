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

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.collection.RawIterator;
import org.neo4j.coreedge.discovery.ClusterTopology;
import org.neo4j.coreedge.discovery.CoreTopologyService;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.BoltAddress;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.ProcedureSignature;

class DiscoverMembersProcedure extends CallableProcedure.BasicProcedure
{
    private final CoreTopologyService discoveryService;

    DiscoverMembersProcedure( CoreTopologyService discoveryService )
    {
        super( new ProcedureSignature(
                new ProcedureSignature.ProcedureName( new String[]{"dbms", "cluster"}, "discoverMembers" ) ) );
        this.discoveryService = discoveryService;
    }

    @Override
    public RawIterator<Object[], ProcedureException> apply( Context ctx, Object[] input ) throws ProcedureException
    {
        final String consistencyLevelInput = input[0].toString();
        EnterpriseCoreEditionModule.ConsistencyLevel consistencyLevel;
        try
        {
            consistencyLevel = EnterpriseCoreEditionModule.ConsistencyLevel.valueOf( consistencyLevelInput );
        }
        catch ( IllegalArgumentException badEnum )
        {
            throw new ProcedureException( Status.Procedure.ProcedureCallFailed, errorMessage( consistencyLevelInput ) );
        }

        ClusterTopology clusterTopology = discoveryService.currentTopology();

        if ( EnterpriseCoreEditionModule.ConsistencyLevel.RYOW_CORE.equals( consistencyLevel ) )
        {
            return wrapUpAddresses( clusterTopology.boltCoreMembers() );
        }
        else // RYOW_EDGE otherwise
        {
            return wrapUpAddresses( clusterTopology.edgeMembers() );
        }
    }

    private static RawIterator<Object[], ProcedureException> wrapUpAddresses( Set<BoltAddress> boltAddresses )
    {
        Stream<AdvertisedSocketAddress> members = boltAddresses.stream().map( BoltAddress::getBoltAddress );
        return Iterators.map( ( l ) -> new Object[]{l.toString()}, Iterators.asRawIterator( members.iterator() ) );
    }

    private String errorMessage( String consistencyLevel )
    {
        return String.format( "Invalid consistency level provided: [%s]. Valid consistency levels are: %s",
                consistencyLevel ,
                Arrays.toString( EnterpriseCoreEditionModule.ConsistencyLevel.values() ) );
    }
}
