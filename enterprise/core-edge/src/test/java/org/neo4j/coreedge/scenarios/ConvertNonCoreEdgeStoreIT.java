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

import junit.framework.Assert;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.coreedge.convert.ConvertClassicStoreCommand;
import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.coreedge.server.core.CoreGraphDatabase;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.neo4j.test.rule.TargetDirectory;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.greaterThan;
import static org.neo4j.coreedge.server.CoreEdgeClusterSettings.raft_advertised_address;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class ConvertNonCoreEdgeStoreIT
{
    @Rule
    public final TargetDirectory.TestDirectory dir = TargetDirectory.testDirForTest( getClass() );

    private Cluster cluster;

    @After
    public void shutdown()
    {
        if ( cluster != null )
        {
            cluster.shutdown();
        }
    }

    @Test
    public void shouldReplicateTransactionToCoreServers() throws Throwable
    {
        // given
        File dbDir = dir.directory();
        FileUtils.deleteRecursively( dbDir );

        File classicNeo4jStore = createClassicNeo4jStore( dbDir, 2000 );

        for ( int serverId = 0; serverId < 3; serverId++ )
        {
            File destination = new File( dbDir, "server-core-" + serverId );
            FileUtils.copyRecursively( classicNeo4jStore, destination );
            new ConvertClassicStoreCommand( destination ).execute();
        }

        cluster = Cluster.start( dbDir, 3, 0 );

        // when
        GraphDatabaseService coreDB = cluster.awaitLeader( 5000 );

        try ( Transaction tx = coreDB.beginTx() )
        {
            Node node = coreDB.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        }

        // then
        for ( final CoreGraphDatabase db : cluster.coreServers() )
        {
            try ( Transaction tx = db.beginTx() )
            {
                ThrowingSupplier<Long, Exception> nodeCount = () -> count( db.getAllNodes() );

                Config config = db.getDependencyResolver().resolveDependency( Config.class );

                assertEventually( "node to appear on core server " + config.get( raft_advertised_address ), nodeCount,
                        greaterThan( 0L ), 15, SECONDS );

                Assert.assertEquals( 2001, count( db.getAllNodes() ) );

                tx.success();
            }
        }
    }

    private File createClassicNeo4jStore( File base, int nodesToCreate )
    {
        File existingDbDir = new File( base, "existing" );
        GraphDatabaseService db = new GraphDatabaseFactory()
                            .newEmbeddedDatabaseBuilder( existingDbDir )
                            .setConfig( GraphDatabaseFacadeFactory.Configuration.record_format, HighLimit.NAME )
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
