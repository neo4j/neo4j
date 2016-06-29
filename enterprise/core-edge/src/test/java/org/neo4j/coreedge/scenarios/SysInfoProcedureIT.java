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

import java.util.List;
import java.util.Optional;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.coreedge.server.core.CoreGraphDatabase;
import org.neo4j.coreedge.server.core.DiscoverMembersProcedure;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.test.coreedge.ClusterRule;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureName;
import static org.neo4j.kernel.api.security.AccessMode.Static.READ;

public class SysInfoProcedureIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule( getClass() )
            .withNumberOfCoreServers( 3 )
            .withNumberOfEdgeServers( 0 );

    private Cluster cluster;

    @Before
    public void setup() throws Exception
    {
        cluster = clusterRule.startCluster();
    }

    @Test
    public void sysInfoProcedureShouldBeAvailable() throws Exception
    {
        Optional<CoreGraphDatabase> database = cluster.coreServers().stream().findFirst();
        KernelAPI kernel = database.get().getDependencyResolver().resolveDependency( KernelAPI.class );
        KernelTransaction transaction = kernel.newTransaction( KernelTransaction.Type.implicit, READ );
        Statement statement = transaction.acquireStatement();

        List<Object[]> members = asList( statement.readOperations().procedureCallRead(
                procedureName( "dbms", "cluster", DiscoverMembersProcedure.NAME ),
                new Object[0] ) );

        assertThat( members, containsInAnyOrder(
                new Object[]{"127.0.0.1:8000"},
                new Object[]{"127.0.0.1:8001"},
                new Object[]{"127.0.0.1:8002"} ) );

    }
}
