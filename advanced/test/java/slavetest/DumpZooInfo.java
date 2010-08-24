package slavetest;

import org.neo4j.kernel.ha.zookeeper.AbstractZooKeeperManager.MachineInfo;
import org.neo4j.kernel.ha.zookeeper.ClusterManager;
import org.neo4j.kernel.ha.zookeeper.NeoStoreUtil;

public class DumpZooInfo
{
    public static void main( String[] args )
    {
        NeoStoreUtil store = new NeoStoreUtil( "var/hadb" );
        ClusterManager clusterManager = new ClusterManager( 
                "localhost", store.getCreationTime(), store.getStoreId(), "" );
        System.out.println( "Master is " + clusterManager.getMaster() );
        System.out.println( "Connected slaves" );
        for ( MachineInfo info : clusterManager.getConnectedSlaves() )
        {
            System.out.println( "\t" + info );
        }
        System.out.println( "Disconnected slaves" );
        for ( MachineInfo info : clusterManager.getDisconnectedSlaves() )
        {
            System.out.println( "\t" + info );
        }
        clusterManager.shutdown();
    }
}
