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

import java.util.concurrent.ExecutionException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.neo4j.coreedge.core.CoreGraphDatabase;
import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.coreedge.discovery.CoreClusterMember;
import org.neo4j.coreedge.discovery.EdgeClusterMember;
import org.neo4j.coreedge.edge.EdgeGraphDatabase;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.enterprise.api.security.EnterpriseAuthSubject;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.test.coreedge.ClusterRule;

import static org.junit.Assert.assertTrue;

public class CoreEdgeProceduresIT
{
    @ClassRule
    public static final ClusterRule clusterRule = new ClusterRule( CoreEdgeProceduresIT.class )
            .withNumberOfCoreMembers( 2 )
            .withNumberOfEdgeMembers( 1 );

    private static Cluster cluster;

    @BeforeClass
    public static void setup() throws Exception
    {
        cluster = clusterRule.startCluster();
    }

    @Test
    public void coreProceduresShouldBeAvailable() throws Throwable
    {
        String[] coreProcs = new String[]{
                "dbms.cluster.role", // Server role
                "dbms.cluster.routing.getServers", // Discover the cluster topology
                "dbms.cluster.overview", // Discover appropriate discovery service
                "dbms.procedures", // Kernel built procedures
//                "dbms.security.listUsers", // Security procedure from community
                "dbms.listQueries" // Built in procedure from enterprise
        };

        for ( String procedure : coreProcs )
        {
            CoreClusterMember coreClusterMember = cluster.coreMembers().stream().findFirst().get();
            CoreGraphDatabase database = coreClusterMember.database();
            InternalTransaction tx =
                    database.beginTransaction( KernelTransaction.Type.explicit, EnterpriseAuthSubject.AUTH_DISABLED );
            Result coreResult = database.execute( "CALL " + procedure + "()" );
            assertTrue( "core with procedure " + procedure, coreResult.hasNext() );
            coreResult.close();
            tx.close();
        }
    }

    @Test
    public void edgeProceduresShouldBeAvailable() throws Exception
    {
        // given
        String[] edgeProcs = new String[]{
                "dbms.cluster.role", // Server role
                "dbms.procedures", // Kernel built procedures
//                "dbms.security.listUsers", // Security procedure from community
                "dbms.listQueries" // Built in procedure from enterprise
        };

        // when
        for ( String procedure : edgeProcs )
        {
            EdgeClusterMember edgeClusterMember = cluster.edgeMembers().stream().findFirst().get();

            EdgeGraphDatabase database = edgeClusterMember.database();
            InternalTransaction tx =
                    database.beginTransaction( KernelTransaction.Type.explicit, EnterpriseAuthSubject.AUTH_DISABLED );
            Result edgeResult = database.execute( "CALL " + procedure + "()" );

            // then
            assertTrue( "edge with procedure " + procedure, edgeResult.hasNext() );
            edgeResult.close();
            tx.close();
        }
    }
}

