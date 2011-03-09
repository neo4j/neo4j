package org.neo4j.server.ext.visualization;

import com.vaadin.Application;
import com.vaadin.terminal.gwt.server.AbstractApplicationServlet;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.EmbeddedGraphDatabase;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

/**
 * Servlet container for the graph visualization.
 */
public class VisualizationServlet extends AbstractApplicationServlet
{
    private GraphDatabaseService graphDb;
    private boolean createEmbeddedGraphDatabase;

    /**
     * No-arg constructor when running outside of Neo4j Server.
     */
    public VisualizationServlet()
    {
        this.createEmbeddedGraphDatabase = true;
    }

    @Override
    public void init( ServletConfig servletConfig ) throws ServletException
    {
        super.init( servletConfig );

        if ( createEmbeddedGraphDatabase )
        {
            String graphDatabaseLocation = getApplicationProperty( "org.neo4j.server.database.location" );
            graphDb = new EmbeddedGraphDatabase(graphDatabaseLocation);
        }
    }

    @Override
    public void destroy()
    {
        super.destroy();
        graphDb.shutdown();
    }

    public VisualizationServlet( GraphDatabaseService graphDb )
    {
        this.graphDb = graphDb;
    }

    @Override
    protected Class<? extends Application> getApplicationClass() throws ClassNotFoundException
    {
        return WidgetTestApplication.class;
    }

    @Override
    protected Application getNewApplication( HttpServletRequest request ) throws ServletException
    {
        return new WidgetTestApplication();
    }
}
