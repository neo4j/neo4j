package org.neo4j.kernel.impl.manage;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

public abstract class Neo4jMonitor
{
    private final ObjectName objectName;

    Neo4jMonitor( int instanceId )
    {
        StringBuilder identifier = new StringBuilder( "org.neo4j:" );
        identifier.append( "instance=kernel#" );
        identifier.append( instanceId );
        identifier.append( ",name=" );
        identifier.append( getName( getClass() ) );
        try
        {
            objectName = new ObjectName( identifier.toString() );
        }
        catch ( MalformedObjectNameException e )
        {
            throw new IllegalArgumentException( e );
        }
    }

    private static Object getName( Class<? extends Neo4jMonitor> clazz )
    {
        for ( Class<?> iface : clazz.getInterfaces() )
        {
            try
            {
                return iface.getField( "NAME" ).get( null );
            }
            catch ( Exception e )
            {
                // Had no NAME field
            }
        }
        throw new IllegalStateException( "Invalid Neo4jMonitor implementation." );
    }

    public ObjectName getObjectName()
    {
        return objectName;
    }
}
