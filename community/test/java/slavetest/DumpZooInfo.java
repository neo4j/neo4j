package slavetest;

import org.neo4j.kernel.ha.zookeeper.ClusterManager;
import org.neo4j.kernel.ha.zookeeper.Machine;

public class DumpZooInfo
{
    public static void main( String[] args )
    {
        ClusterManager clusterManager = new ClusterManager( "localhost" );
        System.out.println( "Master is " + clusterManager.getMaster() );
        System.out.println( "Connected slaves" );
        for ( Machine info : clusterManager.getConnectedSlaves() )
        {
            System.out.println( "\t" + info );
        }
        System.out.println( "Disconnected slaves" );
        for ( Machine info : clusterManager.getDisconnectedSlaves() )
        {
            System.out.println( "\t" + info );
        }
        clusterManager.shutdown();
    }
}
