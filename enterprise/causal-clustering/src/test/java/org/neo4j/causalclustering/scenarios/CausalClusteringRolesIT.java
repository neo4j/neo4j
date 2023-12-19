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
import org.junit.rules.ExpectedException;

import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.WriteOperationsNotAllowedException;
import org.neo4j.test.causalclustering.ClusterRule;

public class CausalClusteringRolesIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule()
            .withNumberOfCoreMembers( 3 )
            .withNumberOfReadReplicas( 1 );

    @Rule
    public ExpectedException exceptionMatcher = ExpectedException.none();

    @Test
    public void readReplicasShouldRefuseWrites() throws Exception
    {
        // given
        Cluster cluster = clusterRule.startCluster();
        GraphDatabaseService db = cluster.findAnyReadReplica().database();
        Transaction tx = db.beginTx();

        // then
        exceptionMatcher.expect( WriteOperationsNotAllowedException.class );

        // when
        db.createNode();
        tx.success();
        tx.close();
    }
}
