package org.neo4j.kernel;

import org.neo4j.graphdb.GraphDatabaseService;

/**
 * Exposes the methods getConfig() and getManagementBean() a.s.o.
 */
public abstract class AbstractGraphDatabase implements GraphDatabaseService
{
    public abstract String getStoreDir();
    
    public abstract Config getConfig();
    
    public abstract <T> T getManagementBean( Class<T> type );
}
