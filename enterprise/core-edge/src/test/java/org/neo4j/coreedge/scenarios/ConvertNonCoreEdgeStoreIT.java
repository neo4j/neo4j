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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.neo4j.coreedge.backup.RestoreClusterCliTest;
import org.neo4j.coreedge.backup.RestoreExistingClusterCli;
import org.neo4j.coreedge.backup.RestoreNewClusterCli;
import org.neo4j.coreedge.core.CoreGraphDatabase;
import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.coreedge.discovery.CoreClusterMember;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0;
import org.neo4j.test.coreedge.ClusterRule;

import static java.util.concurrent.TimeUnit.SECONDS;

import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;

import static org.neo4j.coreedge.backup.ArgsBuilder.args;
import static org.neo4j.coreedge.backup.ArgsBuilder.toArray;
import static org.neo4j.coreedge.backup.RestoreClusterUtils.createClassicNeo4jStore;
import static org.neo4j.coreedge.core.CoreEdgeClusterSettings.raft_advertised_address;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.test.assertion.Assert.assertEventually;

@RunWith(Parameterized.class)
public class ConvertNonCoreEdgeStoreIT
{
    private static final int CLUSTER_SIZE = 3;
    @Rule
    public final ClusterRule clusterRule = new ClusterRule( getClass() )
            .withNumberOfCoreMembers( CLUSTER_SIZE )
            .withNumberOfEdgeMembers( 0 );

    @Parameterized.Parameter()
    public String recordFormat;

    @Parameterized.Parameters(name = "Record format {0}")
    public static Collection<Object> data()
    {
        return Arrays.asList( new Object[]{
                StandardV3_0.NAME, HighLimit.NAME
        } );
    }

    @Test
    public void shouldReplicateTransactionToCoreMembers() throws Throwable
    {
        // given
        File dbDir = clusterRule.testDirectory().cleanDirectory( "classic-db" );
        int classicNodeCount = 1024;
        File classicNeo4jStore = createClassicNeo4jStore( dbDir, classicNodeCount, recordFormat );

        Cluster cluster = this.clusterRule.withRecordFormat( recordFormat ).createCluster();

        Path homeDir = Paths.get(cluster.getCoreMemberById( 0 ).homeDir().getPath());

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrintStream sysOut = new PrintStream( output );

        new RestoreNewClusterCli( homeDir, homeDir, sysOut ).execute(
                toArray( args().from( classicNeo4jStore ).database( "graph.db" ).force().build() )  );

        String seed = RestoreClusterCliTest.extractSeed( output.toString() );

        for ( int serverId = 1; serverId < CLUSTER_SIZE; serverId++ )
        {
            Path destination = Paths.get(cluster.getCoreMemberById( serverId ).homeDir().getPath());

            new RestoreExistingClusterCli( destination, destination ).execute(
                    toArray( args().from( classicNeo4jStore ).database( "graph.db" ).seed( seed ).force().build() )  );
        }

        cluster.start();

        // when
        cluster.coreTx( (coreDB, tx) -> {
            Node node = coreDB.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );

        cluster.addEdgeMemberWithIdAndRecordFormat( 4, recordFormat ).start();

        // then
        for ( final CoreClusterMember server : cluster.coreMembers() )
        {
            CoreGraphDatabase db = server.database();

            try ( Transaction tx = db.beginTx() )
            {
                ThrowingSupplier<Long, Exception> nodeCount = () -> count( db.getAllNodes() );

                Config config = db.getDependencyResolver().resolveDependency( Config.class );

                assertEventually( "node to appear on core server " + config.get( raft_advertised_address ), nodeCount,
                        greaterThan( (long) classicNodeCount ), 15, SECONDS );

                assertEquals( classicNodeCount + 1, count( db.getAllNodes() ) );

                tx.success();
            }
        }
    }
}
