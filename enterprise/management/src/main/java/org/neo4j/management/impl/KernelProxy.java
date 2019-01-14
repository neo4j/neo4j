/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
        List<Object> beans = new ArrayList<>();
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
                if ( className != null )
                {
                    beanType = Class.forName( className );
                }
            }
            catch ( Exception | LinkageError ignored )
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
        Hashtable<String, String> properties = new Hashtable<>();
        properties.put( "instance", "kernel#" + kernelIdentifier );
        return createObjectName( "org.neo4j", properties, beanName, false, extraNaming );
    }

    static ObjectName createObjectNameQuery( String kernelIdentifier, String beanName, String... extraNaming )
    {
        Hashtable<String, String> properties = new Hashtable<>();
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
        Hashtable<String,String> properties = new Hashtable<>( query.getKeyPropertyList() );
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
            if ( query )
            {
                result = ObjectName.getInstance( result.toString() + ",*" );
            }
        }
        catch ( MalformedObjectNameException e )
        {
            return null;
        }
        return result;
    }
}
