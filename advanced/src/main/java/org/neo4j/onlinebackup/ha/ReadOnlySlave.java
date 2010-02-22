package org.neo4j.onlinebackup.ha;

import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.onlinebackup.net.Callback;

public class ReadOnlySlave extends AbstractSlave implements Callback
{
    public ReadOnlySlave( String path, Map<String,String> params, 
        String masterIp, int masterPort )
    {
        super( path, params, masterIp, masterPort );
    }
    
    public GraphDatabaseService getGraphDbService()
    {
        return super.getGraphDb();
    }
}
