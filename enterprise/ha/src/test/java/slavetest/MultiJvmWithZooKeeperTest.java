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
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.neo4j.test.ha.LocalhostZooKeeperCluster;
import org.neo4j.test.ha.StandaloneDatabase;

@Ignore( "Too unstable to have as unit tests. The HA Cronies will make sure the ZooKeeper " +
		"aspect of HA is working correctly" )
public class MultiJvmWithZooKeeperTest extends MultiJvmTest
{
    private static final File BASE_ZOO_KEEPER_DATA_DIR =
            new File( new File( "target" ), "zookeeper-data" );
    private static final int BASE_HA_SERVER_PORT = 5559;

    private static LocalhostZooKeeperCluster zooKeeperCluster;

    private final Map<Integer, StandaloneDatabase> jvmByMachineId = new HashMap<Integer, StandaloneDatabase>();

    @Before
    public void startZooKeeperCluster() throws Exception
    {
        FileUtils.deleteDirectory( BASE_ZOO_KEEPER_DATA_DIR );
        zooKeeperCluster = LocalhostZooKeeperCluster.singleton().clearDataAndVerifyConnection();
    }

    @Override
    protected void initializeDbs( int numSlaves, Map<String,String> config ) throws Exception
    {
        super.initializeDbs( numSlaves, config );
        for ( StandaloneDatabase db : jvmByMachineId.values() )
        {
            db.awaitStarted();
        }
    }

    @Override
    protected StandaloneDatabase spawnJvm( File path, int machineId, String... extraArgs ) throws Exception
    {
        StandaloneDatabase db = StandaloneDatabase.withDefaultBroker( testName.getMethodName(),
                path.getAbsoluteFile(), machineId, zooKeeperCluster,
                buildHaServerConfigValue( machineId ), extraArgs );
        jvmByMachineId.put( machineId + 1, db );
        return db;
    }

    private static String buildHaServerConfigValue( int machineId )
    {
        return "localhost:" + (BASE_HA_SERVER_PORT + machineId);
    }

    @Override
    protected <T> T executeJobOnMaster( Job<T> job ) throws Exception
    {
//        zooKeeperMasterFetcher.waitForSyncConnected();
//        int masterMachineId = zooKeeperMasterFetcher.getCachedMaster().other().getMachineId();
        return jvmByMachineId.get( 1 ).executeJob( job );
    }
}
