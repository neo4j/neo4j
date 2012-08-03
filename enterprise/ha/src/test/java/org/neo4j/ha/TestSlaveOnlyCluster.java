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
package org.neo4j.ha;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.test.TargetDirectory.forTest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.LocalhostZooKeeperCluster;

public class TestSlaveOnlyCluster
{
    private LocalhostZooKeeperCluster zoo;
    private HighlyAvailableGraphDatabase master;
    private final HighlyAvailableGraphDatabase[] slaves = new HighlyAvailableGraphDatabase[2];
    private final TargetDirectory dir = forTest( getClass() );

    @Before
    public void doBefore() throws Exception
    {
        zoo = LocalhostZooKeeperCluster.singleton().clearDataAndVerifyConnection();
        master = new HighlyAvailableGraphDatabase( dir.directory( "master", true ).getAbsolutePath(), stringMap(
                HaSettings.server_id.name(), "0", HaSettings.server.name(), "localhost:" + 6666,
                HaSettings.coordinators.name(), zoo.getConnectionString(), HaSettings.pull_interval.name(),
                0 + "ms" ) );
        for ( int i = 0; i < slaves.length; i++ )
        {
            slaves[i] = new HighlyAvailableGraphDatabase( dir.directory( "" + i+1, true ).getAbsolutePath(), stringMap(
                    HaSettings.server_id.name(), "" + (i+1), HaSettings.server.name(), "localhost:" + ( 6667 + i ),
                    HaSettings.coordinators.name(), zoo.getConnectionString(), HaSettings.pull_interval.name(),
                    0 + "ms", HaSettings.slave_coordinator_update_mode.name(), HaSettings.SlaveUpdateModeSetting.none ) );
        }
    }

    @After
    public void doAfter() throws Exception
    {
        for ( HighlyAvailableGraphDatabase db : slaves )
        {
            if ( db != null ) db.shutdown();
        }
        master.shutdown();
    }

    @Test
    public void testMasterElectionAfterMasterRecoversInSlaveOnlyCluster() throws Exception
    {
        /*
         * Shutdown and start master. Since the other two instances are slaves they will not elect
         * themselves. So when the old master comes back up it should be picked up as the master.
         */
        master.shutdown();
        Thread.sleep( 1000 ); // Make sure everything is shut down, including ZK threads
        master = new HighlyAvailableGraphDatabase( dir.directory( "master", true ).getAbsolutePath(), stringMap(
                HaSettings.server_id.name(), "0", HaSettings.server.name(), "localhost:" + 6666,
                HaSettings.coordinators.name(), zoo.getConnectionString(), HaSettings.pull_interval.name(),
                0 + "ms" ) );
        while (!master.isMaster());
        while (slaves[0].getBroker().getMaster().other().getMachineId() == -1);
        assertEquals(0, slaves[0].getBroker().getMaster().other().getMachineId());
        while (slaves[1].getBroker().getMaster().other().getMachineId() == -1);
        assertEquals(0, slaves[1].getBroker().getMaster().other().getMachineId());
        // Execute a tx on one slave, make sure a master has been picked
        Transaction tx = slaves[0].beginTx();
        slaves[0].createNode();
        tx.success();
        tx.finish();
    }
}
