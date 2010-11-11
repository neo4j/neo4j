package org.neo4j.server.web;

import com.sun.jersey.api.core.ResourceConfig;
import com.sun.jersey.spi.container.WebApplication;
import com.sun.jersey.spi.container.servlet.ServletContainer;
import com.sun.jersey.spi.container.servlet.WebConfig;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.server.database.DatabaseProvider;

public class NeoServletContainer extends ServletContainer
{
    public GraphDatabaseService db;

    public NeoServletContainer( GraphDatabaseService db )
    {
        this.db = db;
    }

    @Override
    protected void configure( WebConfig wc, ResourceConfig rc,
                              WebApplication wa )
    {
        super.configure( wc, rc, wa );

        rc.getSingletons().add( new DatabaseProvider( db ) );
    }
}
