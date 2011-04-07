/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.jmx.impl;

import static java.lang.management.ManagementFactory.getPlatformMBeanServer;

import java.util.Hashtable;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXServiceURL;

import org.neo4j.helpers.Service;
import org.neo4j.jmx.ManagementInterface;
import org.neo4j.kernel.KernelData;

public class ManagementSupport
{
    final static ManagementSupport load()
    {
        ManagementSupport support = new ManagementSupport();
        for ( ManagementSupport candidate : Service.load( ManagementSupport.class ) )
        {
            // Can we know that there aren't going to be multiple instances?
            support = candidate;
        }
        return support;
    }

    protected MBeanServer getMBeanServer()
    {
        return getPlatformMBeanServer();
    }

    /**
     * Create a proxy for the specified bean.
     *
     * @param <T> The type of the bean to create.
     * @param kernel the kernel that the proxy should be created for.
     * @param beanInterface the bean type to create the proxy for.
     * @return a new proxy for the specified bean.
     */
    protected <T> T makeProxy( KernelBean kernel, Class<T> beanInterface )
    {
        throw new UnsupportedOperationException( "Cannot create management bean proxies." );
    }

    protected boolean supportsMxBeans()
    {
        return false;
    }

    /**
     * Get the URI to which connections can be made to the {@link MBeanServer}
     * of this JVM.
     * 
     * @param kernel the kernel that wishes to access the URI.
     * @return a URI that can be used for connecting to the {@link MBeanServer}
     *         of this JVM.
     */
    protected JMXServiceURL getJMXServiceURL( KernelData kernel )
    {
        return null;
    }

    public final ObjectName createObjectName( String instanceId, Class<?> beanInterface )
    {
        return createObjectName( instanceId, getBeanName( beanInterface ) );
    }

    public final ObjectName createMBeanQuery( String instanceId )
    {
        return createObjectName( instanceId, "*" );
    }

    protected String getBeanName( Class<?> beanInterface )
    {
        return beanName( beanInterface );
    }

    protected ObjectName createObjectName( String instanceId, String beanName )
    {
        Hashtable<String, String> properties = new Hashtable<String, String>();
        properties.put( "instance", "kernel#" + instanceId );
        properties.put( "name", beanName );
        try
        {
            return new ObjectName( "org.neo4j", properties );
        }
        catch ( MalformedObjectNameException e )
        {
            return null;
        }
    }

    static String beanName( Class<?> iface )
    {
        if ( iface.isInterface() )
        {
            ManagementInterface management = iface.getAnnotation( ManagementInterface.class );
            if ( management != null )
            {
                return management.name();
            }
        }
        throw new IllegalArgumentException( iface + " is not a Neo4j Management Been interface" );
    }
}
