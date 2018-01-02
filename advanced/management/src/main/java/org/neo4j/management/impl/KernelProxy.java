/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.management.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Hashtable;
import java.util.List;
import java.util.NoSuchElementException;

import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;

import org.neo4j.jmx.ManagementInterface;

/**
 * Does not have any public methods - since the public interface of
 * {@link org.neo4j.management.Neo4jManager} should be defined completely in
 * that class.
 *
 * Does not have any (direct or transitive) dependencies on any part of the jmx
 * component - since this class is used in
 * {@link org.neo4j.management.impl.jconsole.Neo4jPlugin the JConsole plugin},
 * and the jmx component is not on the class path in JConsole.
 *
 * @author Tobias Ivarsson <tobias.ivarsson@neotechnology.com>
 */
public abstract class KernelProxy
{
    static final String KERNEL_BEAN_TYPE = "org.neo4j.jmx.Kernel";
    protected static final String KERNEL_BEAN_NAME = "Kernel";
    static final String MBEAN_QUERY = "MBeanQuery";
    protected final MBeanServerConnection server;
    protected final ObjectName kernel;

    protected KernelProxy( MBeanServerConnection server, ObjectName kernel )
    {
        String className = null;
        try
        {
            className = server.getMBeanInfo( kernel ).getClassName();
        }
        catch ( Exception e )
        {
            // fall through
        }
        if ( !KERNEL_BEAN_TYPE.equals( className ) )
        {
            throw new IllegalArgumentException(
                    "The specified ObjectName does not represent a Neo4j Kernel bean in the specified MBean server." );
        }
        this.server = server;
        this.kernel = kernel;
    }

    protected List<Object> allBeans()
    {
        List<Object> beans = new ArrayList<Object>();
        Iterable<ObjectInstance> mbeans;
        try
        {
            mbeans = server.queryMBeans( mbeanQuery(), null );
        }
        catch ( IOException handled )
        {
            return beans;
        }
        for ( ObjectInstance instance : mbeans )
        {
            String className = instance.getClassName();
            Class<?> beanType = null;
            try
            {
                if ( className != null ) beanType = Class.forName( className );
            }
            catch ( Exception ignored )
            {
                // fall through
            }
            catch ( LinkageError ignored )
            {
                // fall through
            }
            if ( beanType != null )
            {
                try
                {
                    beans.add( BeanProxy.load( server, beanType, instance.getObjectName() ) );
                }
                catch ( Exception ignored )
                {
                    // fall through
                }
            }
        }
        return beans;
    }

    private ObjectName assertExists( ObjectName name )
    {
        try
        {
            if ( !server.queryNames( name, null ).isEmpty() )
            {
                return name;
            }
        }
        catch ( IOException handled )
        {
            // fall through
        }
        throw new NoSuchElementException( "No MBeans matching " + name );
    }

    protected <T> T getBean( Class<T> beanInterface )
    {
        return BeanProxy.load( server, beanInterface, createObjectName( beanInterface ) );
    }

    protected <T> Collection<T> getBeans( Class<T> beanInterface )
    {
        return BeanProxy.loadAll( server, beanInterface, createObjectNameQuery( beanInterface ) );
    }

    private ObjectName createObjectNameQuery( Class<?> beanInterface )
    {
        return createObjectNameQuery( mbeanQuery(), beanInterface );
    }

    private ObjectName createObjectName( Class<?> beanInterface )
    {
        return assertExists( createObjectName( mbeanQuery(), beanInterface ) );
    }

    protected ObjectName createObjectName( String beanName )
    {
        return assertExists( createObjectName( mbeanQuery(), beanName, false ) );
    }

    protected ObjectName mbeanQuery()
    {
        try
        {
            return (ObjectName) server.getAttribute( kernel, MBEAN_QUERY );
        }
        catch ( Exception cause )
        {
            throw new IllegalStateException( "Could not get MBean query.", cause );
        }
    }

    protected static ObjectName createObjectName( String kernelIdentifier, Class<?> beanInterface )
    {
        return createObjectName( kernelIdentifier, beanName( beanInterface ) );
    }

    protected static ObjectName createObjectName( String kernelIdentifier, String beanName, String... extraNaming )
    {
        Hashtable<String, String> properties = new Hashtable<String, String>();
        properties.put( "instance", "kernel#" + kernelIdentifier );
        return createObjectName( "org.neo4j", properties, beanName, false, extraNaming );
    }

    static ObjectName createObjectNameQuery( String kernelIdentifier, String beanName, String... extraNaming )
    {
        Hashtable<String, String> properties = new Hashtable<String, String>();
        properties.put( "instance", "kernel#" + kernelIdentifier );
        return createObjectName( "org.neo4j", properties, beanName, true, extraNaming );
    }

    static ObjectName createObjectName( ObjectName query, Class<?> beanInterface )
    {
        return createObjectName( query, beanName( beanInterface ), false );
    }

    static ObjectName createObjectNameQuery( ObjectName query, Class<?> beanInterface )
    {
        return createObjectName( query, beanName( beanInterface ), true );
    }

    private static ObjectName createObjectName( ObjectName query, String beanName, boolean isQuery )
    {
        Hashtable<String, String> properties = new Hashtable<String, String>(query.getKeyPropertyList());
        return createObjectName( query.getDomain(), properties, beanName, isQuery );
    }

    static String beanName( Class<?> beanInterface )
    {
        if ( beanInterface.isInterface() )
        {
            ManagementInterface management = beanInterface.getAnnotation( ManagementInterface.class );
            if ( management != null )
            {
                return management.name();
            }
        }
        throw new IllegalArgumentException( beanInterface + " is not a Neo4j Management Been interface" );
    }

    private static ObjectName createObjectName( String domain, Hashtable<String, String> properties, String beanName,
            boolean query, String... extraNaming )
    {
        properties.put( "name", beanName );
        for ( int i = 0; i < extraNaming.length; i++ )
        {
            properties.put( "name" + i, extraNaming[i] );
        }
        ObjectName result;
        try
        {
            result = new ObjectName( domain, properties );
            if ( query ) result = ObjectName.getInstance( result.toString() + ",*" );
        }
        catch ( MalformedObjectNameException e )
        {
            return null;
        }
        return result;
    }
}
