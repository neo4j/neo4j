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
package org.neo4j.coreedge.scenarios;

import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.coreedge.discovery.CoreClusterMember;
import org.neo4j.coreedge.discovery.HazelcastDiscoveryServiceFactory;
import org.neo4j.coreedge.discovery.procedures.DiscoverEndpointAcquisitionServersProcedure;
import org.neo4j.coreedge.messaging.address.AdvertisedSocketAddress;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.test.coreedge.ClusterRule;

import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureName;
import static org.neo4j.kernel.api.security.AccessMode.Static.READ;

public class ClusterMembershipChangeIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule( getClass() ).withDiscoveryServiceFactory( new HazelcastDiscoveryServiceFactory() )
            .withNumberOfCoreMembers( 3 );

    @Test
    public void newMemberNotInInitialMembersConfig() throws Throwable
    {
        // when
        Cluster cluster = clusterRule.withNumberOfEdgeMembers( 0 ).startCluster();

        List<AdvertisedSocketAddress> onlyServerZero = singletonList( Cluster.socketAddressForServer( 0 ) );

        // then
        cluster.addCoreMemberWithIdAndInitialMembers( 3, onlyServerZero ).start();
        cluster.addCoreMemberWithIdAndInitialMembers( 4, onlyServerZero ).start();
        cluster.addCoreMemberWithIdAndInitialMembers( 5, onlyServerZero ).start();

        cluster.removeCoreMemberWithMemberId( 0 );
        cluster.removeCoreMemberWithMemberId( 1 );
        cluster.removeCoreMemberWithMemberId( 2 );

        cluster.shutdown();
        cluster.start();

        List<Object[]> currentMembers;
        for ( CoreClusterMember member: cluster.coreMembers() )
        {
            currentMembers = discoverClusterMembers( member.database() );
            assertThat( currentMembers, containsInAnyOrder(
                    new Object[]{"127.0.0.1:8003"},
                    new Object[]{"127.0.0.1:8004"},
                    new Object[]{"127.0.0.1:8005"} ) );
        }
    }

    private List<Object[]> discoverClusterMembers( GraphDatabaseFacade db )
            throws TransactionFailureException, org.neo4j.kernel.api.exceptions.ProcedureException
    {
        KernelAPI kernel = db.getDependencyResolver().resolveDependency( KernelAPI.class );
        KernelTransaction transaction = kernel.newTransaction( KernelTransaction.Type.implicit, READ );
        Statement statement = transaction.acquireStatement();

        // when
        return asList( statement.readOperations().procedureCallRead(
                procedureName( "dbms", "cluster", DiscoverEndpointAcquisitionServersProcedure.NAME ), new Object[0] ) );
    }
}
