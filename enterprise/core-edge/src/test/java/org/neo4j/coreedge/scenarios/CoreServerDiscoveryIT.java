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

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.coreedge.discovery.DiscoveryServiceFactory;
import org.neo4j.coreedge.discovery.HazelcastDiscoveryServiceFactory;
import org.neo4j.coreedge.discovery.TestOnlyDiscoveryServiceFactory;
import org.neo4j.coreedge.server.core.CoreGraphDatabase;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.KernelTransaction.Type;
import org.neo4j.kernel.api.Statement;
import org.neo4j.test.rule.TargetDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;

import static org.neo4j.coreedge.server.core.EnterpriseCoreEditionModule.ConsistencyLevel.RYOW_CORE;
import static org.neo4j.coreedge.server.core.EnterpriseCoreEditionModule.ConsistencyLevel.RYOW_EDGE;
import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureName;
import static org.neo4j.kernel.api.security.AccessMode.Static.READ;

@RunWith( Parameterized.class )
public class CoreServerDiscoveryIT
{
    @Rule
    public final TargetDirectory.TestDirectory dir = TargetDirectory.testDirForTest( getClass() );

    private Cluster cluster;

    @Parameterized.Parameter
    public DiscoveryServiceFactory discoveryServiceFactory;

    @Parameterized.Parameters( name = "{0}" )
    public static Iterable<Object[]> params()
    {
        return Arrays.asList( new Object[][]{
                {new HazelcastDiscoveryServiceFactory()},
                {new TestOnlyDiscoveryServiceFactory()},
        } );
    }

    @After
    public void shutdown() throws ExecutionException, InterruptedException
    {
        if ( cluster != null )
        {
            cluster.shutdown();
            cluster = null;
        }
    }

    @Test
    public void shouldDiscoverClusterMembers() throws Exception
    {
        // given
        File dbDir = dir.directory();
        cluster = Cluster.start( dbDir, 3, 2, discoveryServiceFactory );

        CoreGraphDatabase db = cluster.getCoreServerById( 0 );
        KernelAPI kernel = db.getDependencyResolver().resolveDependency( KernelAPI.class );
        KernelTransaction transaction = kernel.newTransaction( Type.implicit, READ );
        Statement statement = transaction.acquireStatement();

        // when
        List<Object[]> levels = asList( statement.readOperations()
                .procedureCallRead( procedureName( "dbms", "cluster", "discoverConsistencyLevels" ), new Object[0] ) );

        // then
        assertThat( levels, containsInAnyOrder(
                new Object[]{RYOW_CORE.name()},
                new Object[]{RYOW_EDGE.name()}
        ) );

        // when
        List<Object[]> coreMembers = asList( statement.readOperations().procedureCallRead(
                procedureName( "dbms", "cluster", "discoverMembers" ),
                new Object[]{RYOW_CORE.name()} ) );

        // then
        assertEquals( 3, coreMembers.size() );

        assertThat( coreMembers, containsInAnyOrder(
                new Object[]{"127.0.0.1:8000"},
                new Object[]{"127.0.0.1:8001"},
                new Object[]{"127.0.0.1:8002"}
        ) );

        // when
        List<Object[]> edgeMembers = asList( statement.readOperations().procedureCallRead(
                procedureName( "dbms", "cluster", "discoverMembers" ),
                new Object[]{RYOW_EDGE.name()} ) );

        // then
        assertEquals( 2, edgeMembers.size() );

        assertThat( edgeMembers, containsInAnyOrder(
                new Object[]{"127.0.0.1:9000"},
                new Object[]{"127.0.0.1:9001"}
        ) );
    }
}
