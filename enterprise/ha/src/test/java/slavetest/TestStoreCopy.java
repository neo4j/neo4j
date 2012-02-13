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

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.HAGraphDb;
import org.neo4j.kernel.HaConfig;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.LocalhostZooKeeperCluster;

/**
 * Check sandboxed copy of stores. The before and after methods
 * as they are form a complete test, so a test method with an empty
 * body would still perform a valid test.
 */
public class TestStoreCopy
{
    private static LocalhostZooKeeperCluster zoo;

    private HAGraphDb master;
    private HAGraphDb slave;
    private File slaveDir;
    private File sandboxed;
    private long nodeId;

    @BeforeClass
    public static void startZoo()
    {
        zoo = new LocalhostZooKeeperCluster( TestStoreCopy.class, new int[] {
                3181, 3182, 3183 } );
    }

    @AfterClass
    public static void stopZoo()
    {
        zoo.shutdown();
    }

    /**
     * Starts a master, creates a node and sets a property, starts the slave and
     * checks successful copy of the store.
     *
     * @throws Exception
     */
    @Before
    public void setupMachinesAndSanityCheck() throws Exception
    {
        slaveDir = TargetDirectory.forTest( TestStoreCopy.class ).directory(
                "slave-sandboxed", true );
        sandboxed = new File( slaveDir, HAGraphDb.COPY_FROM_MASTER_TEMP );

        master = new HAGraphDb(
                TargetDirectory.forTest( TestStoreCopy.class ).directory(
                        "master-sandboxed", true ).getAbsolutePath(),
                MapUtil.stringMap(
                        HaConfig.CONFIG_KEY_COORDINATORS,
                        zoo.getConnectionString(),
                        HaConfig.CONFIG_KEY_SERVER_ID, "1" ) );

        Transaction masterTx = master.beginTx();
        Node n = master.createNode();
        n.setProperty( "foo", "bar" );
        nodeId = n.getId();
        masterTx.success();
        masterTx.finish();

        slave = new HAGraphDb( slaveDir.getAbsolutePath(),
                MapUtil.stringMap( HaConfig.CONFIG_KEY_COORDINATORS,
                        zoo.getConnectionString(),
                        HaConfig.CONFIG_KEY_SERVER_ID, "2" ) );

        Assert.assertEquals( 1,
                master.getBroker().getMaster().other().getMachineId() );
        Assert.assertEquals( 1,
                slave.getBroker().getMaster().other().getMachineId() );

        // Simple sanity check
        Assert.assertEquals( "bar",
                slave.getNodeById( nodeId ).getProperty( "foo" ) );
    }

    /**
     * This is the sandboxed copy test, so we naturally want to always make sure
     * that the sandbox directory after every startup is there and holds only
     * messages.log
     *
     * Also, shutdown the instances.
     *
     * @throws Exception
     */
    @After
    public void shutdownAndCheckSandbox() throws Exception
    {
        slave.shutdown();
        master.shutdown();

        /*
         * Make sure:
         *  - The sandbox directory exists
         *  - It only has one file in it
         *  - It is the messages.log file
         */
        Assert.assertTrue( sandboxed.exists() );
        Assert.assertTrue( sandboxed.isDirectory() );
        Assert.assertEquals( 1, sandboxed.listFiles().length );
        Assert.assertEquals( StringLogger.DEFAULT_NAME,
                sandboxed.listFiles()[0].getName() );
    }

    /**
     * Checks that leftovers in the sandbox directory are discarded on store
     * copy.
     *
     * @throws Exception
     */
    @Test
    public void sandboxIsOverwritten() throws Exception
    {
        slave.shutdown();

        Transaction secondMasterTx = master.beginTx();
        Node n = master.getNodeById( nodeId );
        n.setProperty( "foo2", "bar2" );
        secondMasterTx.success();
        secondMasterTx.finish();

        File sandboxed = new File( slaveDir, HAGraphDb.COPY_FROM_MASTER_TEMP );
        FileUtils.moveToDirectory( new File( slaveDir, "neostore" ), sandboxed,
                false );
        FileUtils.moveToDirectory( new File( slaveDir,
                "neostore.propertystore.db" ), sandboxed, false );
        Assert.assertEquals( 3, sandboxed.listFiles().length );

        slave = new HAGraphDb( slaveDir.getAbsolutePath(), MapUtil.stringMap(
                HaConfig.CONFIG_KEY_COORDINATORS, zoo.getConnectionString(),
                HaConfig.CONFIG_KEY_SERVER_ID, "2" ) );

        Assert.assertEquals( "bar",
                slave.getNodeById( nodeId ).getProperty( "foo" ) );
        Assert.assertEquals( "bar2",
                slave.getNodeById( nodeId ).getProperty( "foo2" ) );
    }
}
