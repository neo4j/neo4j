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
import java.util.Collection;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.coreedge.discovery.CoreServer;
import org.neo4j.coreedge.server.core.CoreGraphDatabase;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0;
import org.neo4j.restore.RestoreClusterCliTest;
import org.neo4j.restore.RestoreClusterUtils;
import org.neo4j.restore.RestoreExistingClusterCli;
import org.neo4j.restore.RestoreNewClusterCli;
import org.neo4j.test.coreedge.ClusterRule;

import static java.util.concurrent.TimeUnit.SECONDS;

import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;

import static org.neo4j.coreedge.server.CoreEdgeClusterSettings.raft_advertised_address;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.restore.ArgsBuilder.args;
import static org.neo4j.restore.ArgsBuilder.toArray;
import static org.neo4j.test.assertion.Assert.assertEventually;

@RunWith(Parameterized.class)
public class ConvertNonCoreEdgeStoreIT
{
    private static final int CLUSTER_SIZE = 3;
    @Rule
    public final ClusterRule clusterRule = new ClusterRule( getClass() )
            .withNumberOfCoreServers( CLUSTER_SIZE )
            .withNumberOfEdgeServers( 0 );

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
    public void shouldReplicateTransactionToCoreServers() throws Throwable
    {
        // given
        File dbDir = clusterRule.testDirectory().cleanDirectory( "classic-db" );
        File classicNeo4jStore = createClassicNeo4jStore( dbDir, 10, recordFormat );

        Cluster cluster = this.clusterRule.withRecordFormat( recordFormat ).createCluster();

        File homeDir = cluster.getCoreServerById( 0 ).homeDir();

        String output = RestoreClusterUtils.execute( () -> RestoreNewClusterCli.main( toArray( args()
                .homeDir( homeDir ).config( homeDir ).from( classicNeo4jStore )
                .database( "graph.db" ).force().build() ) ) );

        String seed = RestoreClusterCliTest.extractSeed( output );

        for ( int serverId = 1; serverId < CLUSTER_SIZE; serverId++ )
        {
            File destination = cluster.getCoreServerById( serverId ).homeDir();
            RestoreClusterUtils.execute( () -> RestoreExistingClusterCli.main( toArray( args().homeDir( destination )
                    .config( destination ).from( classicNeo4jStore ).database( "graph.db" ).seed( seed ).force().build() ) ) );
        }

        cluster.start();

        // when
        CoreGraphDatabase coreDB = cluster.awaitLeader( 5000 ).database();

        try ( Transaction tx = coreDB.beginTx() )
        {
            Node node = coreDB.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        }

        cluster.addEdgeServerWithIdAndRecordFormat( 4, recordFormat ).start();

        // then
        for ( final CoreServer server : cluster.coreServers() )
        {
            CoreGraphDatabase db = server.database();

            try ( Transaction tx = db.beginTx() )
            {
                ThrowingSupplier<Long, Exception> nodeCount = () -> count( db.getAllNodes() );

                Config config = db.getDependencyResolver().resolveDependency( Config.class );

                assertEventually( "node to appear on core server " + config.get( raft_advertised_address ), nodeCount,
                        greaterThan( 0L ), 15, SECONDS );

                assertEquals( 11, count( db.getAllNodes() ) );

                tx.success();
            }
        }
    }

    private File createClassicNeo4jStore( File base, int nodesToCreate, String recordFormat )
    {
        File existingDbDir = new File( base, "existing" );
        GraphDatabaseService db = new GraphDatabaseFactory()
                            .newEmbeddedDatabaseBuilder( existingDbDir )
                            .setConfig( GraphDatabaseSettings.record_format, recordFormat )
                            .newGraphDatabase();

        for ( int i = 0; i < (nodesToCreate / 2); i++ )
        {
            try ( Transaction tx = db.beginTx() )
            {
                Node node1 = db.createNode( Label.label( "Label-" + i ) );
                Node node2 = db.createNode( Label.label( "Label-" + i ) );
                node1.createRelationshipTo( node2, RelationshipType.withName( "REL-" + i ) );
                tx.success();
            }
        }

        db.shutdown();

        return existingDbDir;
    }
}
