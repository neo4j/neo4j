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
package org.neo4j.kernel.ha;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.test.TargetDirectory.forTest;

import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.LocalhostZooKeeperCluster;

public class TestTxPush
{
    private final TargetDirectory dir = forTest( getClass() );
    private LocalhostZooKeeperCluster zoo;

    @Test
    public void testMasterCapableIsAheadOfSlaveOnlyRegardlessOfPriority() throws Exception
    {
        HighlyAvailableGraphDatabase master = null,
                slave1 = null,
                slave2 = null;
        try
        {
            zoo = LocalhostZooKeeperCluster.singleton().clearDataAndVerifyConnection();
            master = new HighlyAvailableGraphDatabase( dir.directory( "master", true ).getAbsolutePath(), stringMap(
                    HaSettings.server_id.name(), "0", HaSettings.server.name(), "localhost:" + 6666,
                    HaSettings.coordinators.name(), zoo.getConnectionString(), HaSettings.pull_interval.name(),
                    0 + "ms", HaSettings.tx_push_strategy.name(), HaSettings.TxPushStrategySetting.fixed , HaSettings.tx_push_factor.name(), "1") );
            slave1 = new HighlyAvailableGraphDatabase( dir.directory( "slave1", true ).getAbsolutePath(), stringMap(
                    HaSettings.server_id.name(), "1", HaSettings.server.name(), "localhost:" + 6667,
                    HaSettings.coordinators.name(), zoo.getConnectionString(), HaSettings.pull_interval.name(),
                    0 + "ms", HaSettings.slave_coordinator_update_mode.name(), HaSettings.SlaveUpdateModeSetting.none ) );
            slave2 = new HighlyAvailableGraphDatabase( dir.directory( "slave2", true ).getAbsolutePath(), stringMap(
                    HaSettings.server_id.name(), "2", HaSettings.server.name(), "localhost:" + 6668,
                    HaSettings.coordinators.name(), zoo.getConnectionString(), HaSettings.pull_interval.name(),
                    0 + "ms") );

            Transaction tx = master.beginTx();
            Node node = master.createNode();
            long nodeId = node.getId();
            node.setProperty( "foo", "bar" );
            tx.success();
            tx.finish();

            try
            {
                slave1.getNodeById( nodeId );
                fail("It shouldn't be in the slave only instance");
            }
            catch ( NotFoundException e )
            {
                // not there, as supposed to
            }

            assertEquals( "bar", slave2.getNodeById( nodeId ).getProperty( "foo" ) );
        }
        finally
        {
            if ( slave2 != null )
            {
                slave2.shutdown();
            }
            if ( slave1 != null )
            {
                slave1.shutdown();
            }
            if ( master != null )
            {
                master.shutdown();
            }
        }
    }

    @Test
    public void testSlaveOnlyWillNotGetPushedAtToMeetQuota() throws Exception
    {
        HighlyAvailableGraphDatabase master = null,
                slave1 = null,
                slave2 = null;
        try
        {
            zoo = LocalhostZooKeeperCluster.singleton().clearDataAndVerifyConnection();
            master = new HighlyAvailableGraphDatabase( dir.directory( "master", true ).getAbsolutePath(), stringMap(
                    HaSettings.server_id.name(), "0", HaSettings.server.name(), "localhost:" + 6666,
                    HaSettings.coordinators.name(), zoo.getConnectionString(), HaSettings.pull_interval.name(),
                    0 + "ms", HaSettings.tx_push_strategy.name(), HaSettings.TxPushStrategySetting.fixed , HaSettings.tx_push_factor.name(), "2") );
            slave1 = new HighlyAvailableGraphDatabase( dir.directory( "slave1", true ).getAbsolutePath(), stringMap(
                    HaSettings.server_id.name(), "1", HaSettings.server.name(), "localhost:" + 6667,
                    HaSettings.coordinators.name(), zoo.getConnectionString(), HaSettings.pull_interval.name(),
                    0 + "ms", HaSettings.slave_coordinator_update_mode.name(), HaSettings.SlaveUpdateModeSetting.none ) );
            slave2 = new HighlyAvailableGraphDatabase( dir.directory( "slave2", true ).getAbsolutePath(), stringMap(
                    HaSettings.server_id.name(), "2", HaSettings.server.name(), "localhost:" + 6668,
                    HaSettings.coordinators.name(), zoo.getConnectionString(), HaSettings.pull_interval.name(),
                    0 + "ms") );

            Transaction tx = master.beginTx();
            Node node = master.createNode();
            long nodeId = node.getId();
            node.setProperty( "foo", "bar" );
            tx.success();
            tx.finish();

            /*
             * This is the slave only, it will not get transactions pushed at even though it is in higher prio and
             * the push factor is 2
             */

            try
            {
                slave1.getNodeById( nodeId );
                fail("It shouldn't be in the slave only instance");
            }
            catch ( NotFoundException e )
            {
                // fine
            }

            assertEquals( "bar", slave2.getNodeById( nodeId ).getProperty( "foo" ) );
        }
        finally
        {
            if ( slave2 != null )
            {
                slave2.shutdown();
            }
            if ( slave1 != null )
            {
                slave1.shutdown();
            }
            if ( master != null )
            {
                master.shutdown();
            }
        }
    }
}