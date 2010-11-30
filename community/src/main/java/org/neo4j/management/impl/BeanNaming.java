package org.neo4j.management.impl;

import java.util.Hashtable;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

class BeanNaming
{
    private BeanNaming()
    {
    }

    static ObjectName getObjectName( String instanceId, Class<?> beanInterface, String beanName )
    {
        final String name = beanName( beanInterface, beanName );
        if ( name == null ) return null;
        StringBuilder identifier = new StringBuilder( "org.neo4j:" );
        identifier.append( "instance=kernel#" );
        identifier.append( instanceId == null ? "*" : instanceId );
        identifier.append( ",name=" );
        identifier.append( name );
        try
        {
            return new ObjectName( identifier.toString() );
        }
        catch ( MalformedObjectNameException e )
        {
            return null;
        }
    }

    public static ObjectName getObjectName( ObjectName beanQuery, Class<?> beanInterface,
            String beanName )
    {
        String name = beanName( beanInterface, beanName );
        if ( name == null ) return null;
        Hashtable<String, String> properties = new Hashtable<String, String>(
                beanQuery.getKeyPropertyList() );
        properties.put( "name", name );
        try
        {
            return new ObjectName( beanQuery.getDomain(), properties );
        }
        catch ( Exception e )
        {
            throw new IllegalStateException( "Could not create specified MBean Query." );
        }
    }

    private static String beanName( Class<?> beanInterface, String beanName )
    {
        final String name;
        if ( beanName != null )
        {
            name = beanName;
        }
        else if ( beanInterface == null )
        {
            name = "*";
        }
        else
        {
            try
            {
                name = (String) beanInterface.getField( "NAME" ).get( null );
            }
            catch ( Exception e )
            {
                return null;
            }
        }
        return name;
    }
}
