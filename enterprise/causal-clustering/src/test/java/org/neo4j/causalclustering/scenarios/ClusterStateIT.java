/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.scenarios;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.util.concurrent.TimeUnit;

import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.causalclustering.ClusterRule;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.test.rule.fs.FileSystemRule;

import static org.junit.Assert.assertNotEquals;
import static org.neo4j.causalclustering.discovery.Cluster.dataMatchesEventually;

public class ClusterStateIT
{
    private final ClusterRule clusterRule = new ClusterRule()
            .withNumberOfCoreMembers( 3 )
            .withNumberOfReadReplicas( 0 );
    private final FileSystemRule fileSystemRule = new DefaultFileSystemRule();
    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( fileSystemRule ).around( clusterRule );

    @Test
    public void shouldRecreateClusterStateIfStoreIsMissing() throws Throwable
    {
        // given
        FileSystemAbstraction fs = fileSystemRule.get();
        Cluster cluster = clusterRule.startCluster();
        cluster.awaitLeader();

        cluster.coreTx( ( db, tx ) ->
        {
            SampleData.createData( db, 100 );
            tx.success();
        } );
        CoreClusterMember follower = cluster.awaitCoreMemberWithRole( Role.FOLLOWER, 5, TimeUnit.SECONDS );
        MemberId followerId = follower.id();
        // when
        follower.shutdown();
        fs.deleteRecursively( follower.storeDir() );
        follower.start();

        // then
        assertNotEquals( "MemberId should have changed", followerId, follower.id() );
        dataMatchesEventually( follower, cluster.coreMembers() );
    }

}
