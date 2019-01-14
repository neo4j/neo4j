/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.scenarios;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.causalclustering.discovery.ReadReplica;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.test.causalclustering.ClusterRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;

public class ClusterFormationIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule()
            .withNumberOfCoreMembers( 3 )
            .withNumberOfReadReplicas( 0 );

    private Cluster cluster;

    @Before
    public void setup() throws Exception
    {
        cluster = clusterRule.startCluster();
    }

    @Test
    public void shouldSupportBuiltInProcedures()
    {
        cluster.addReadReplicaWithId( 0 ).start();

        Stream.concat(
            cluster.readReplicas().stream().map( ReadReplica::database),
            cluster.coreMembers().stream().map(CoreClusterMember::database)
        ).forEach( gdb ->
        {
            // (1) BuiltInProcedures from community
            {
                Result result = gdb.execute( "CALL dbms.procedures()" );
                assertTrue( result.hasNext() );
                result.close();
            }

            // (2) BuiltInProcedures from enterprise
            try ( InternalTransaction tx = gdb.beginTransaction(
                    KernelTransaction.Type.explicit,
                    EnterpriseLoginContext.AUTH_DISABLED
            ) )
            {
                Result result = gdb.execute( tx, "CALL dbms.listQueries()", EMPTY_MAP );
                assertTrue( result.hasNext() );
                result.close();

                tx.success();
            }
        } );
    }

    @Test
    public void shouldBeAbleToAddAndRemoveCoreMembers()
    {
        // when
        cluster.getCoreMemberById( 0 ).shutdown();
        cluster.getCoreMemberById( 0 ).start();

        // then
        assertEquals( 3, cluster.numberOfCoreMembersReportedByTopology() );

        // when
        cluster.removeCoreMemberWithServerId( 1 );

        // then
        assertEquals( 2, cluster.numberOfCoreMembersReportedByTopology() );

        // when
        cluster.addCoreMemberWithId( 4 ).start();

        // then
        assertEquals( 3, cluster.numberOfCoreMembersReportedByTopology() );
    }

    @Test
    public void shouldBeAbleToAddAndRemoveCoreMembersUnderModestLoad()
    {
        // given
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit( () ->
        {
            CoreGraphDatabase leader = cluster.getMemberWithRole( Role.LEADER ).database();
            try ( Transaction tx = leader.beginTx() )
            {
                leader.createNode();
                tx.success();
            }
        } );

        // when
        cluster.getCoreMemberById( 0 ).shutdown();
        cluster.getCoreMemberById( 0 ).start();

        // then
        assertEquals( 3, cluster.numberOfCoreMembersReportedByTopology() );

        // when
        cluster.removeCoreMemberWithServerId( 0 );

        // then
        assertEquals( 2, cluster.numberOfCoreMembersReportedByTopology() );

        // when
        cluster.addCoreMemberWithId( 4 ).start();

        // then
        assertEquals( 3, cluster.numberOfCoreMembersReportedByTopology() );

        executorService.shutdown();
    }

    @Test
    public void shouldBeAbleToRestartTheCluster() throws Exception
    {
        // when started then
        assertEquals( 3, cluster.numberOfCoreMembersReportedByTopology() );

        // when
        cluster.shutdown();
        cluster.start();

        // then
        assertEquals( 3, cluster.numberOfCoreMembersReportedByTopology() );

        // when
        cluster.removeCoreMemberWithServerId( 1 );

        cluster.addCoreMemberWithId( 3 ).start();
        cluster.shutdown();

        cluster.start();

        // then
        assertEquals( 3, cluster.numberOfCoreMembersReportedByTopology() );
    }
}
