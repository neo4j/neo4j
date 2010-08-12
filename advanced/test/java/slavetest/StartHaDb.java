package slavetest;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;

public class StartHaDb
{
    public static void main( String[] args ) throws Exception
    {
        final GraphDatabaseService db = new HighlyAvailableGraphDatabase( "var/hadb", MapUtil.stringMap(
                HighlyAvailableGraphDatabase.CONFIG_KEY_HA_MACHINE_ID, "2",
                HighlyAvailableGraphDatabase.CONFIG_KEY_HA_ZOO_KEEPER_SERVERS, "172.16.2.33:2181,172.16.1.242:2181,172.16.4.14:2181",
                HighlyAvailableGraphDatabase.CONFIG_KEY_HA_SERVERS, "1=172.16.2.33:5559,2=172.16.1.242:5559",
                "enable_remote_shell", "true" ) );
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            public void run()
            {
                db.shutdown();
            }
        } );
    }
}
