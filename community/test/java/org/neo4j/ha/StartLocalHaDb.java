package org.neo4j.ha;

import java.util.Map;

import org.junit.Ignore;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;

@Ignore
public class StartLocalHaDb
{
    public static void main( String[] args )
    {
        String path = args[0];
        String configFile = args[1];
        Map<String, String> config = HighlyAvailableGraphDatabase.loadConfigurations( configFile );
        final GraphDatabaseService graphDb = new HighlyAvailableGraphDatabase( path, config );
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                graphDb.shutdown();
            }
        } );
    }
}
