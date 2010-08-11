package slavetest;

import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;

public class DemoWithZooBroker
{
    public static void main( String args[] )
    {
        new EmbeddedGraphDatabase( "target/ha1" ).shutdown();
        Map<String,String> config = MapUtil.stringMap( 
            HighlyAvailableGraphDatabase.CONFIG_KEY_HA_MACHINE_ID, "1",
            HighlyAvailableGraphDatabase.CONFIG_KEY_HA_ZOO_KEEPER_SERVERS, "localhost:2181,localhost:2182,localhost:2183", 
            HighlyAvailableGraphDatabase.CONFIG_KEY_HA_SERVERS, "1=localhost:5559,2=localhost:5560" );
        GraphDatabaseService graphDb = new HighlyAvailableGraphDatabase( "target/ha1", config );
        graphDb.shutdown();
    }
}
