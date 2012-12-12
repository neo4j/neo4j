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

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.HighlyAvailableGraphDatabase.NOT_ALLOWED_TO_JOIN_CLUSTER_WITH_EMPTY_STORE;
import static org.neo4j.test.TargetDirectory.forTest;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.HaConfig;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.LocalhostZooKeeperCluster;

public class TestInstanceJoin
{
    private static LocalhostZooKeeperCluster zoo;
    private final TargetDirectory dir = forTest( getClass() );
    private HighlyAvailableGraphDatabase master, slave;

    @BeforeClass
    public static void startZoo() throws Exception
    {
        zoo = LocalhostZooKeeperCluster.singleton();
    }

    @Before
    public void cleanZoo() throws Exception
    {
        zoo.clearDataAndVerifyConnection();
    }

    @Test
    public void shouldAllowSlaveToJoinWithEmptyStore() throws Exception
    {
        startAndPopulateMaster();

        startSlave( true );

        assertThat( (String) master.getReferenceNode().getProperty( "someProperty" ), is( "someValue" ) );
        assertThat( (String) slave.getReferenceNode().getProperty( "someProperty" ), is( "someValue" ) );
    }

    @Test
    public void shouldAllowSlaveToJoinWithOutdatedData() throws Exception
    {
        startAndPopulateMaster();
        startSlave( true );
        slave.shutdown();
        updateMaster();

        startSlave( false );

        assertThat( (String) master.getReferenceNode().getProperty( "someProperty" ), is( "AnUpdatedValue" ) );
        assertThat( (String) slave.getReferenceNode().getProperty( "someProperty" ), is( "AnUpdatedValue" ) );
    }

    @Test
    public void shouldAllowSlaveToJoinWithOutdatedDataEvenWhenPeersUnavailable() throws Exception
    {
        startAndPopulateMaster();
        startSlave( true );
        slave.shutdown();
        updateMaster();
        assertThat( (String) master.getReferenceNode().getProperty( "someProperty" ), is( "AnUpdatedValue" ) );
        master.shutdown();

        startSlave( false );

        assertThat( (String) slave.getReferenceNode().getProperty( "someProperty" ), is( "someValue" ) );
    }

    @Test
    public void shouldNotAllowSlaveToJoinWithEmptyStoreWhenPeersUnavailable() throws Exception
    {
        startAndPopulateMaster();
        master.shutdown();

        try
        {
            startSlave( true );

            fail();
        }
        catch ( IllegalStateException e )
        {
            assertThat( e.getMessage(), is( NOT_ALLOWED_TO_JOIN_CLUSTER_WITH_EMPTY_STORE ) );
        }
    }

    @After
    public void shutDownInstances() throws Exception
    {
        try
        {
            if ( slave != null )
            {
                slave.shutdown();
            }
        }
        catch ( IllegalStateException e )
        {

        }

        try
        {
            if ( master != null )
            {
                master.shutdown();
            }
        }
        catch ( IllegalStateException e )
        {

        }
    }

    private void startAndPopulateMaster()
    {
        HighlyAvailableGraphDatabase master = start( dir.directory( "master", true ).getAbsolutePath(), 1,
                zoo.getConnectionString() );
        Transaction tx = master.beginTx();
        master.getReferenceNode().setProperty( "someProperty", "someValue" );
        tx.success();
        tx.finish();

        this.master = master;
    }

    private void startSlave( boolean clean ) throws InterruptedException
    {
        HighlyAvailableGraphDatabase slave = start( dir.directory( "slave", clean ).getAbsolutePath(), 2,
                zoo.getConnectionString() );

        // let it catch up
        Thread.sleep( 100 );

        this.slave = slave;
    }

    private void updateMaster()
    {
        Transaction tx = master.beginTx();
        master.getReferenceNode().setProperty( "someProperty", "AnUpdatedValue" );
        tx.success();
        tx.finish();
    }

    private static HighlyAvailableGraphDatabase start( String storeDir, int i, String zkConnectString )
    {
        return new HighlyAvailableGraphDatabase( storeDir, stringMap( HaConfig.CONFIG_KEY_SERVER_ID, "" + i,
                HaConfig.CONFIG_KEY_SERVER, "localhost:" + (6666 + i), HaConfig.CONFIG_KEY_COORDINATORS,
                zkConnectString, HaConfig.CONFIG_KEY_PULL_INTERVAL, 10 + "ms" ) );
    }
}
