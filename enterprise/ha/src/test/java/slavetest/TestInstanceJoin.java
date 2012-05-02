/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.test.TargetDirectory.forTest;

import java.io.File;

import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.HaConfig;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.LocalhostZooKeeperCluster;

/*
 * This test case ensures that instances with the same store id but very old txids
 * will successfully join with a full version of the store.
 */
public class TestInstanceJoin
{
    private static LocalhostZooKeeperCluster zoo;
    private final TargetDirectory dir = forTest( getClass() );

    @BeforeClass
    public static void startZoo() throws Exception
    {
        zoo = LocalhostZooKeeperCluster.singleton().clearDataAndVerifyConnection();
    }

    @Test
    public void testIt() throws Exception
    {
        /*
         * First create a cluster. Then have some txs on the master. Rotate to create history logs so we
         * can delete them and make them inaccessible to the slave, making it like they are very old.
         */
        HighlyAvailableGraphDatabase master = start( dir.directory( "master", true ).getAbsolutePath(), 1,
                zoo.getConnectionString() );
        Transaction tx = master.beginTx();
        master.getReferenceNode().setProperty( "foo", "bar" );
        tx.success();
        tx.finish();

        master.shutdown(); // rotate FTW

        /*
         * This second transaction is so this instance will win in the upcoming election.
         */
        master = start( dir.directory( "master", false ).getAbsolutePath(), 1, zoo.getConnectionString() );
        tx = master.beginTx();
        master.createNode();
        tx.success();
        tx.finish();

        master.shutdown();

        /*
         * Delete the logical logs, thus faking old entries no longer present.
         */
        new File( dir.directory( "master", false ), "nioneo_logical.log.v0" ).delete();
        new File( dir.directory( "master", false ), "nioneo_logical.log.v1" ).delete();

        /*
         * Slave starts, finds itself alone in a cluster with an existing store id. Uses that, becomes master
         * for now.
         */
        HighlyAvailableGraphDatabase slave = start( dir.directory( "slave", true ).getAbsolutePath(), 2,
                zoo.getConnectionString() );

        /*
         * Do a tx so we have something to ask for.
         */
        tx = slave.beginTx();
        slave.createNode();
        tx.success();
        tx.finish();

        /*
         * Why shutdown here since a master switch is enough? Well, i am glad you asked. If we
         * just startup the master, indeed it will do a master switch and fail on ensureDataConsistency.
         * BUT, this will happen after the test case is over, since it is asynchronous. So we would have
         * to Thread.sleep() for around 5 seconds, which is ugly. Instead, we shutdown, allow the master
         * to come up and then try to join, which is synchronous and will fail in the constructor, making
         * the test failure clear and reproducible. Downside, it has an additional shutdown/startup cycle
         * which takes some time.
         */
        slave.shutdown();

        master = start( dir.directory( "master", false ).getAbsolutePath(), 1,
                zoo.getConnectionString() );

        /*
         * The slave should successfully start up and get the proper store.
         */
        slave = start( dir.directory( "slave", false ).getAbsolutePath(), 2, zoo.getConnectionString() );

        assertEquals( "store contents don't seem to be the same", "bar", slave.getReferenceNode().getProperty( "foo" ) );

        slave.shutdown();
        master.shutdown();
    }

    private static HighlyAvailableGraphDatabase start( String storeDir, int i, String zkConnectString )
    {
        return new HighlyAvailableGraphDatabase( storeDir, stringMap( HaConfig.CONFIG_KEY_SERVER_ID, "" + i,
                HaConfig.CONFIG_KEY_SERVER, "localhost:" + ( 6666 + i ), HaConfig.CONFIG_KEY_COORDINATORS,
                zkConnectString, HaConfig.CONFIG_KEY_PULL_INTERVAL, 0 + "ms" ) );
    }
}
