package slavetest;

import org.neo4j.kernel.ha.zookeeper.ClusterManager;
import org.neo4j.kernel.ha.zookeeper.NeoStoreUtil;

public class DumpZooInfo
{
    public static void main( String[] args )
    {
        NeoStoreUtil store = new NeoStoreUtil( "var/hadb" );
        ClusterManager clusterManager = new ClusterManager( 
                "localhost", store.getCreationTime(), store.getStoreId() );
        clusterManager.dumpInfo();
        clusterManager.close();
    }
}
