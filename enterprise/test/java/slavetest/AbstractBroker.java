package slavetest;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.impl.ha.Broker;

abstract class AbstractBroker implements Broker
{
    private GraphDatabaseService db;
    
    public void setDb( GraphDatabaseService db )
    {
        this.db = db;
    }
    
    public GraphDatabaseService getDb()
    {
        return this.db;
    }
}
