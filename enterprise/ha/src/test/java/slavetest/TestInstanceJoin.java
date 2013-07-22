/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package slavetest;

import static org.junit.Assert.assertEquals;
import static org.neo4j.com.ServerUtil.rotateLogs;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.keep_logical_logs;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.test.TargetDirectory.forTest;

import java.util.Map;

import org.junit.Test;
import org.neo4j.cluster.ClusterSettings;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.UpdatePuller;
import org.neo4j.test.TargetDirectory;

/*
 * This test case ensures that instances with the same store id but very old txids
 * will successfully join with a full version of the store.
 */
public class TestInstanceJoin
{
    private final TargetDirectory dir = forTest( getClass() );

    @Test
    public void makeSureSlaveCanJoinEvenIfTooFarBackComparedToMaster() throws Exception
    {
        String key = "foo";
        String value = "bar";

        HighlyAvailableGraphDatabase master = null;
        HighlyAvailableGraphDatabase slave = null;
        try
        {
            master = start( dir.directory( "master", true ).getAbsolutePath(), 0, stringMap( keep_logical_logs.name(),
                    "1 files", ClusterSettings.initial_hosts.name(), "127.0.0.1:5001" ) );
            createNode( master, "something", "unimportant" );
            // Need to start and shutdown the slave so when we start it up later it verifies instead of copying
            slave = start( dir.directory( "slave", true ).getAbsolutePath(), 1,
                    stringMap( ClusterSettings.initial_hosts.name(), "127.0.0.1:5001,127.0.0.1:5002" ) );
            slave.shutdown();

            long nodeId = createNode( master, key, value );
            createNode( master, "something", "unimportant" );
            // Rotating, moving the above transactions away so they are removed on shutdown.
            rotateLogs( master );

            /*
             * We need to shutdown - rotating is not enough. The problem is that log positions are cached and they
             * are not removed from the cache until we run into the cache limit. This means that the information
             * contained in the log can actually be available even if the log is removed. So, to trigger the case
             * of the master information missing from the master we need to also flush the log entry cache - hence,
             * restart.
             */
            master.shutdown();
            master = start( dir.directory( "master", false ).getAbsolutePath(), 0, stringMap( keep_logical_logs.name(),
                    "1 files", ClusterSettings.initial_hosts.name(), "127.0.0.1:5001" ) );

            slave = start( dir.directory( "slave", false ).getAbsolutePath(), 1,
                    stringMap( ClusterSettings.initial_hosts.name(), "127.0.0.1:5001,127.0.0.1:5002" ) );
            slave.getDependencyResolver().resolveDependency( UpdatePuller.class ).pullUpdates();

            Transaction transaction = slave.beginTx();
            try
            {
                assertEquals( "store contents differ", value, slave.getNodeById( nodeId ).getProperty( key ) );
            }
            finally
            {
                transaction.finish();
            }
        }
        finally
        {
            if ( slave != null )
            {
                slave.shutdown();
            }

            if ( master != null )
            {
                master.shutdown();
            }
        }
    }

    private long createNode( HighlyAvailableGraphDatabase db, String key, String value )
    {
        Transaction tx = db.beginTx();
        Node node = db.createNode();
        node.setProperty( key, value );
        tx.success();
        tx.finish();
        return node.getId();
    }

    private static HighlyAvailableGraphDatabase start( String storeDir, int i )
    {
        return start( storeDir, i, stringMap() );
    }

    private static HighlyAvailableGraphDatabase start( String storeDir, int i, Map<String, String> additionalConfig )
    {
        HighlyAvailableGraphDatabase db = (HighlyAvailableGraphDatabase) new HighlyAvailableGraphDatabaseFactory().
                newHighlyAvailableDatabaseBuilder( storeDir )
                .setConfig( ClusterSettings.cluster_server, "127.0.0.1:" + (5001 + i) )
                .setConfig( ClusterSettings.server_id, i + "" )
                .setConfig( HaSettings.ha_server, "127.0.0.1:" + (6666 + i) )
                .setConfig( HaSettings.pull_interval, "0ms" )
                .setConfig( additionalConfig )
                .newGraphDatabase();

        awaitStart( db );
        return db;
    }

    private static void awaitStart( HighlyAvailableGraphDatabase db )
    {
        db.beginTx().finish();
    }
}
