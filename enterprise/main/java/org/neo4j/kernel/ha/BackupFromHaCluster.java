package org.neo4j.kernel.ha;

import java.util.Map;

import org.neo4j.helpers.Args;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.zookeeper.ClusterManager;
import org.neo4j.kernel.ha.zookeeper.Machine;

public class BackupFromHaCluster
{
    public static void main( String[] args ) throws Exception
    {
        Args arguments = new Args( args );
        String storeDir = arguments.get( "path",
                !arguments.orphans().isEmpty() ? arguments.orphans().get( 0 ) : null );
        ClusterManager cluster = new ClusterManager( arguments.get(
                HighlyAvailableGraphDatabase.CONFIG_KEY_HA_ZOO_KEEPER_SERVERS, null ) );
        cluster.waitForSyncConnected();
        
        final Machine master = cluster.getCachedMaster().other();
        System.out.println( "Master:" + master );
        Map<String, String> config = MapUtil.stringMap(
                HighlyAvailableGraphDatabase.CONFIG_KEY_HA_MACHINE_ID,
                String.valueOf( master.getMachineId() ) );
        HighlyAvailableGraphDatabase db = new HighlyAvailableGraphDatabase( storeDir, config,
                AbstractBroker.wrapSingleBroker( new BackupBroker( new MasterClient(
                        master.getServer().first(), master.getServer().other() ) ) ) );
        System.out.println( "Leaching backup from master " + master );
        try
        {
            db.pullUpdates();
            System.out.println( "Backup completed successfully" );
        }
        finally
        {
            db.shutdown();
            cluster.shutdown();
        }
    }
}
