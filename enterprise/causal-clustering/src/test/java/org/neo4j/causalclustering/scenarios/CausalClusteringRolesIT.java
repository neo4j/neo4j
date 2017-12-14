/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
