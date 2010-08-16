package slavetest;

import java.io.File;
import java.util.Date;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.zookeeper.NeoStoreUtil;

public class StartHaDb
{
    public static final File PATH = new File( "var/hadb" );
    
    static final Map<Integer, String> HA_SERVERS = MapUtil.genericMap(
            "1", "172.16.2.33:5559",
            "2", "172.16.1.242:5559"
    );
    
    public static void main( String[] args ) throws Exception
    {
        NeoStoreUtil store = new NeoStoreUtil( PATH.getPath() );
        System.out.println( "Starting store: createTime=" + new Date( store.getCreationTime() ) + 
                " identifier=" + store.getStoreId() + " last committed tx=" + store.getLastCommittedTx() );
        final GraphDatabaseService db = new HighlyAvailableGraphDatabase( PATH.getPath(), MapUtil.stringMap(
                HighlyAvailableGraphDatabase.CONFIG_KEY_HA_MACHINE_ID, "1",
                HighlyAvailableGraphDatabase.CONFIG_KEY_HA_ZOO_KEEPER_SERVERS, join( StartZooKeeperServer.ZOO_KEEPER_SERVERS, "," ),
                HighlyAvailableGraphDatabase.CONFIG_KEY_HA_SERVERS, toHaServerFormat( HA_SERVERS ),
                "enable_remote_shell", "true",
                Config.KEEP_LOGICAL_LOGS, "true" ) );
//        Runtime.getRuntime().addShutdownHook( new Thread()
//        {
//            public void run()
//            {
//                db.shutdown();
//            }
//        } );
    }
    
    private static String toHaServerFormat( Map<Integer, String> haServers )
    {
        StringBuilder builder = new StringBuilder();
        for ( Map.Entry<Integer, String> entry : haServers.entrySet() )
        {
            builder.append( (builder.length() > 0 ? "," : "") + entry.getKey() + "=" + entry.getValue() );
        }
        return builder.toString();
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
