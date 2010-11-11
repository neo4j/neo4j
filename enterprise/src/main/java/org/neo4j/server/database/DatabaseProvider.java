package org.neo4j.server.database;

import com.sun.jersey.api.core.HttpContext;
import org.neo4j.graphdb.GraphDatabaseService;

import javax.ws.rs.ext.Provider;

@Provider
public class DatabaseProvider extends
        AbstractInjectableProvider<GraphDatabaseService>
{
    public GraphDatabaseService db;

    public DatabaseProvider( GraphDatabaseService db )
    {
        super( GraphDatabaseService.class );
        this.db = db;
    }

    @Override
    public GraphDatabaseService getValue( HttpContext httpContext )
    {
        return db;
    }
}
