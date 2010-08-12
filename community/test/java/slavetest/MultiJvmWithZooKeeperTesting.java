package slavetest;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.zookeeper.ClusterManager;
import org.neo4j.kernel.ha.zookeeper.NeoStoreUtil;

public class MultiJvmWithZooKeeperTesting extends MultiJvmTesting
{
    private static final File BASE_ZOO_KEEPER_DATA_DIR =
            new File( new File( "target" ), "zookeeper-data" );
    private static final int BASE_HA_SERVER_PORT = 5559;
    private static final int ZOO_KEEPER_CLUSTER_SIZE = 3;
    
    private static LocalZooKeeperCluster zooKeeperCluster;
    
    private ClusterManager zooKeeperMasterFetcher;
    private Map<Integer, StandaloneDbCom> jvmByMachineId;
    
    @BeforeClass
    public static void startZooKeeperCluster() throws Exception
    {
        FileUtils.deleteDirectory( BASE_ZOO_KEEPER_DATA_DIR );
        zooKeeperCluster = new LocalZooKeeperCluster( ZOO_KEEPER_CLUSTER_SIZE,
                LocalZooKeeperCluster.defaultDataDirectoryPolicy( BASE_ZOO_KEEPER_DATA_DIR ),
                LocalZooKeeperCluster.defaultPortPolicy( 2181 ),
                LocalZooKeeperCluster.defaultPortPolicy( 2888 ),
                LocalZooKeeperCluster.defaultPortPolicy( 3888 ) );
    }
    
    @Override
    protected void initializeDbs( int numSlaves ) throws Exception
    {
        this.jvmByMachineId = new HashMap<Integer, StandaloneDbCom>();
        super.initializeDbs( numSlaves );
        NeoStoreUtil store = new NeoStoreUtil( SKELETON_DB_PATH.getAbsolutePath() );
        zooKeeperMasterFetcher = new ClusterManager(
                buildZooKeeperServersConfigValue( ZOO_KEEPER_CLUSTER_SIZE ),
                store.getCreationTime(), store.getStoreId() );
    }
    
    @Override
    protected StandaloneDbCom spawnJvm( int numServers, File path, int port, int machineId,
            String... extraArgs ) throws Exception
    {
        List<String> myExtraArgs = new ArrayList<String>();
        myExtraArgs.add( "-" + HighlyAvailableGraphDatabase.CONFIG_KEY_HA_MACHINE_ID );
        myExtraArgs.add( "" + (machineId+1) );
        myExtraArgs.add( "-" + HighlyAvailableGraphDatabase.CONFIG_KEY_HA_ZOO_KEEPER_SERVERS );
        myExtraArgs.add( buildZooKeeperServersConfigValue( ZOO_KEEPER_CLUSTER_SIZE ) );
        myExtraArgs.add( "-" + HighlyAvailableGraphDatabase.CONFIG_KEY_HA_SERVERS );
        myExtraArgs.add( buildHaServersConfigValue( numServers ) );
        myExtraArgs.addAll( Arrays.asList( extraArgs ) );
        StandaloneDbCom com = super.spawnJvm( numServers, path, port, machineId,
                myExtraArgs.toArray( new String[myExtraArgs.size()] ) );
        com.awaitStarted();
        jvmByMachineId.put( com.getMachineId(), com );
        return com;
    }
    
    private String buildHaServersConfigValue( int numServers )
    {
        StringBuilder builder = new StringBuilder();
        for ( int i = 0; i < numServers; i++ )
        {
            builder.append( (i > 0 ? "," : "") + (i+1) + "=localhost:" +
                    (BASE_HA_SERVER_PORT + i) );
        }
        return builder.toString();
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
        int masterMachineId = zooKeeperMasterFetcher.getMaster();
        return jvmByMachineId.get( masterMachineId ).executeJob( job );
    }
    
    @AfterClass
    public static void shutdownZooKeeperCluster()
    {
        zooKeeperCluster.shutdown();
    }
}
