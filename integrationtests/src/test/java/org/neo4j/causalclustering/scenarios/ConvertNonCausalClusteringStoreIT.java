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

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.causalclustering.helpers.ClassicNeo4jStore;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.restore.RestoreDatabaseCommand;
import org.neo4j.test.causalclustering.ClusterRule;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.raft_advertised_address;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.test.assertion.Assert.assertEventually;

@RunWith( Parameterized.class )
public class ConvertNonCausalClusteringStoreIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule()
            .withNumberOfCoreMembers( 3 )
            .withNumberOfReadReplicas( 0 );

    @Parameterized.Parameter()
    public String recordFormat;

    @Parameterized.Parameters( name = "Record format {0}" )
    public static Collection<Object> data()
    {
        return Arrays.asList( new Object[]{Standard.LATEST_NAME, HighLimit.NAME} );
    }

    @Test
    public void shouldReplicateTransactionToCoreMembers() throws Throwable
    {
        // given
        File dbDir = clusterRule.testDirectory().cleanDirectory( "classic-db-" + recordFormat );
        int classicNodeCount = 1024;
        File classicNeo4jStore = createNeoStore( dbDir, classicNodeCount );

        Cluster cluster = this.clusterRule.withRecordFormat( recordFormat ).createCluster();

        try ( DefaultFileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction() )
        {
            for ( CoreClusterMember core : cluster.coreMembers() )
            {
                new RestoreDatabaseCommand( fileSystem, classicNeo4jStore, core.config(), core.settingValue(
                        GraphDatabaseSettings.active_database.name() ), true ).execute();
            }
        }

        cluster.start();

        // when
        cluster.coreTx( ( coreDB, tx ) ->
        {
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
                ThrowingSupplier<Long,Exception> nodeCount = () -> count( db.getAllNodes() );

                Config config = db.getDependencyResolver().resolveDependency( Config.class );

                assertEventually( "node to appear on core server " + config.get( raft_advertised_address ), nodeCount,
                        greaterThan( (long) classicNodeCount ), 15, SECONDS );

                assertEquals( classicNodeCount + 1, count( db.getAllNodes() ) );

                tx.success();
            }
        }
    }

    private File createNeoStore( File dbDir, int classicNodeCount ) throws IOException
    {
        try ( DefaultFileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction() )
        {
            return ClassicNeo4jStore.builder( dbDir, fileSystem ).amountOfNodes( classicNodeCount ).recordFormats( recordFormat ).build().getStoreDir();
        }
    }
}
