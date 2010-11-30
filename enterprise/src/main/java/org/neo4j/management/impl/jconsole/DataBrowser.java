package org.neo4j.management.impl.jconsole;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;

import javax.swing.JLabel;
import javax.swing.JPanel;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.management.RemoteConnection;

class DataBrowser extends Widget
{
    private final GraphDatabaseService graphDb;

    DataBrowser( RemoteConnection remote ) throws ClassNotFoundException, SecurityException,
            NoSuchMethodException, IllegalAccessException, IllegalArgumentException
    {
        Class<?> jmxTarget = Class.forName( "org.neo4j.remote.transports.JmxTarget" );
        Method connect = jmxTarget.getMethod( "connectGraphDatabase", RemoteConnection.class );
        try
        {
            this.graphDb = (GraphDatabaseService) connect.invoke( null, remote );
        }
        catch ( InvocationTargetException e )
        {
            throw launderRuntimeException( e.getTargetException() );
        }
    }

    static RuntimeException launderRuntimeException( Throwable exception )
    {
        if ( exception instanceof RuntimeException )
        {
            return (RuntimeException) exception;
        }
        else if ( exception instanceof Error )
        {
            throw (Error) exception;
        }
        else
        {
            throw new RuntimeException( "Unexpected Exception!", exception );
        }
    }

    @Override
    void populate( JPanel view )
    {
        view.add( new JLabel( "Place holder for the Neo4j data viewer" ) );
    }

    @Override
    void dispose()
    {
        graphDb.shutdown();
    }

    @Override
    void update( Collection<UpdateEvent> result )
    {
        // TODO tobias: Implement update() [Nov 30, 2010]
    }
}
