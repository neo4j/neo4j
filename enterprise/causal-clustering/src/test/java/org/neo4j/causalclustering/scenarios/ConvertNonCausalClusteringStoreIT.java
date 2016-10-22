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
package org.neo4j.causalclustering.scenarios;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0;
import org.neo4j.test.causalclustering.ClusterRule;

import static java.util.concurrent.TimeUnit.SECONDS;

import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;

import static org.neo4j.causalclustering.backup.RestoreClusterUtils.createClassicNeo4jStore;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.raft_advertised_address;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.test.assertion.Assert.assertEventually;

@RunWith(Parameterized.class)
public class ConvertNonCausalClusteringStoreIT
{
    private static final int CLUSTER_SIZE = 3;
    @Rule
    public final ClusterRule clusterRule = new ClusterRule( getClass() )
            .withNumberOfCoreMembers( CLUSTER_SIZE )
            .withNumberOfReadReplicas( 0 );

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
        File classicNeo4jStore = createClassicNeo4jStore( dbDir, new DefaultFileSystemAbstraction(), classicNodeCount, recordFormat );

        Cluster cluster = this.clusterRule.withRecordFormat( recordFormat ).createCluster();

        for ( int serverId = 0; serverId < CLUSTER_SIZE; serverId++ )
        {
            File destination = cluster.getCoreMemberById( serverId ).storeDir();
            FileUtils.copyRecursively( classicNeo4jStore, destination );
        }

        cluster.start();

        // when
        cluster.coreTx( (coreDB, tx) -> {
            Node node = coreDB.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );

        cluster.addReadReplicaWithIdAndRecordFormat( 4, recordFormat ).start();

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
