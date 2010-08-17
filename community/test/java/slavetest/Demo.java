package slavetest;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;

public class Demo extends SingleJvmTesting
{
    public static void main( String args[] ) throws Exception
    {
        Demo demo = new Demo();
        demo.initializeDbs( 1 );
        GraphDatabaseService slave1 = demo.getSlave( 0 );
        // GraphDatabaseService slave2 = demo.getSlave( 1 );
        GraphDatabaseService master = demo.getMaster().getGraphDb();

        master.enableRemoteShell();
        Map<String,Serializable> shellConfig = new HashMap<String, Serializable>();
        shellConfig.put( "port", Integer.valueOf( 1338 ) );
        slave1.enableRemoteShell( shellConfig );
//        shellConfig.put( "port", Integer.valueOf( 1339 ) );
//        slave2.enableRemoteShell( shellConfig );
    }
}
