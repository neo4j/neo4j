package slavetest;

import java.io.File;
import java.util.Date;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.zookeeper.NeoStoreUtil;

public class BringUpOther
{
    public static void main( String[] args ) throws Exception
    {
        File path = new File( "test/dbs/1" );
        NeoStoreUtil store = new NeoStoreUtil( path.getPath() );
        System.out.println( "Starting store: createTime=" + new Date( store.getCreationTime() ) +
                " identifier=" + store.getStoreId() + " last committed tx=" + store.getLastCommittedTx() );
        final GraphDatabaseService db = new HighlyAvailableGraphDatabase( path.getPath(), MapUtil.stringMap(
                HighlyAvailableGraphDatabase.CONFIG_KEY_HA_MACHINE_ID, "2",
                HighlyAvailableGraphDatabase.CONFIG_KEY_HA_ZOO_KEEPER_SERVERS, "localhost:2181,localhost:2182,localhost:2183",
                HighlyAvailableGraphDatabase.CONFIG_KEY_HA_SERVER, "localhost:5560",
                Config.ENABLE_REMOTE_SHELL, "port=1338",
                Config.KEEP_LOGICAL_LOGS, "true" ) );
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            public void run()
            {
                db.shutdown();
            }
        } );
        System.out.println( "up" );
        while ( true ) Thread.sleep( 1000 );
    }
}
