/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.zookeeper.ClusterManager;
import org.neo4j.kernel.impl.LocalZooKeeperCluster;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Ignore
public class MultiJvmWithZooKeeperTest extends MultiJvmTest
{
    private static final File BASE_ZOO_KEEPER_DATA_DIR =
            new File( new File( "target" ), "zookeeper-data" );
    private static final int BASE_HA_SERVER_PORT = 5559;
    private static final int ZOO_KEEPER_CLUSTER_SIZE = 3;
    
    private static LocalZooKeeperCluster zooKeeperCluster;
    
    private ClusterManager zooKeeperMasterFetcher;
    private Map<Integer, StandaloneDbCom> jvmByMachineId;
    
    @Before
    public void startZooKeeperCluster() throws Exception
    {
        FileUtils.deleteDirectory( BASE_ZOO_KEEPER_DATA_DIR );
        zooKeeperCluster = new LocalZooKeeperCluster( ZOO_KEEPER_CLUSTER_SIZE, BASE_ZOO_KEEPER_DATA_DIR );
    }
    
    @Override
    protected void initializeDbs( int numSlaves, Map<String,String> config ) throws Exception
    {
        this.jvmByMachineId = new HashMap<Integer, StandaloneDbCom>();
        super.initializeDbs( numSlaves, config );
        zooKeeperMasterFetcher = new ClusterManager(
                buildZooKeeperServersConfigValue( ZOO_KEEPER_CLUSTER_SIZE ) );
        for ( StandaloneDbCom db : jvmByMachineId.values() )
        {
            db.awaitStarted();
        }
    }
    
    @Override
    protected StandaloneDbCom spawnJvm( File path, int port, int machineId,
            String... extraArgs ) throws Exception
    {
        List<String> myExtraArgs = new ArrayList<String>();
        myExtraArgs.add( "-" + HighlyAvailableGraphDatabase.CONFIG_KEY_HA_MACHINE_ID );
        myExtraArgs.add( "" + (machineId+1) );
        myExtraArgs.add( "-" + HighlyAvailableGraphDatabase.CONFIG_KEY_HA_ZOO_KEEPER_SERVERS );
        myExtraArgs.add( buildZooKeeperServersConfigValue( ZOO_KEEPER_CLUSTER_SIZE ) );
        myExtraArgs.add( "-" + HighlyAvailableGraphDatabase.CONFIG_KEY_HA_SERVER );
        myExtraArgs.add( buildHaServerConfigValue( machineId ) );
        myExtraArgs.addAll( Arrays.asList( extraArgs ) );
        StandaloneDbCom com = super.spawnJvm( path, port, machineId,
                myExtraArgs.toArray( new String[myExtraArgs.size()] ) );
        jvmByMachineId.put( machineId+1, com );
        return com;
    }
    
    private static String buildHaServerConfigValue( int machineId )
    {
        return "localhost:" + (BASE_HA_SERVER_PORT + machineId);
    }

    private static String buildZooKeeperServersConfigValue( int zooKeeperClusterSize )
    {
        StringBuilder builder = new StringBuilder();
        for ( int i = 0; i < zooKeeperClusterSize; i++ )
        {
            builder.append( (i > 0 ? "," : "") + "localhost:" +
                    zooKeeperCluster.getClientPortPolicy().getPort( i+1 ) );
        }
        return builder.toString();
    }
    
    @Override
    protected <T> T executeJobOnMaster( Job<T> job ) throws Exception
    {
//        zooKeeperMasterFetcher.waitForSyncConnected();
//        int masterMachineId = zooKeeperMasterFetcher.getCachedMaster().other().getMachineId();
        return jvmByMachineId.get( 1 ).executeJob( job );
    }
    
    @After
    public void shutdownZooKeeperCluster()
    {
        zooKeeperCluster.shutdown();
    }
}
