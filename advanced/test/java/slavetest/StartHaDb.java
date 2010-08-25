package slavetest;

import java.io.File;
import java.util.Date;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.zookeeper.NeoStoreUtil;

public class StartHaDb
{
    public static final File PATH = new File( "var/hadb" );

    static final String HA_SERVER = "172.16.1.242:5559";
        
//            "1", "172.16.2.33:5559", // JS
//            "2", "172.16.1.242:5559", // MP
//            "3", "172.16.4.14:5559" // TI

    public static void main( String[] args ) throws Exception
    {
        NeoStoreUtil store = new NeoStoreUtil( PATH.getPath() );
        System.out.println( "Starting store: createTime=" + new Date( store.getCreationTime() ) +
                " identifier=" + store.getStoreId() + " last committed tx=" + store.getLastCommittedTx() );
        final GraphDatabaseService db = new HighlyAvailableGraphDatabase( PATH.getPath(), MapUtil.stringMap(
                HighlyAvailableGraphDatabase.CONFIG_KEY_HA_MACHINE_ID, "2",
                HighlyAvailableGraphDatabase.CONFIG_KEY_HA_ZOO_KEEPER_SERVERS, join( StartZooKeeperServer.ZOO_KEEPER_SERVERS, "," ),
                HighlyAvailableGraphDatabase.CONFIG_KEY_HA_SERVER, HA_SERVER,
                Config.ENABLE_REMOTE_SHELL, "true",
                Config.KEEP_LOGICAL_LOGS, "true" ) );
//        Runtime.getRuntime().addShutdownHook( new Thread()
//        {
//            public void run()
//            {
//                db.shutdown();
//            }
//        } );
    }

    private static String join( String[] strings, String delimiter )
    {
        StringBuilder builder = new StringBuilder();
        for ( String string : strings )
        {
            builder.append( (builder.length() > 0 ? delimiter : "") + string );
        }
        return builder.toString();
    }
}
