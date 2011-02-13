/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import javax.management.DynamicMBean;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.KernelData;

public abstract class ManagementBeanProvider extends Service
{
    final Class<?> beanInterface;
    private final String beanName;

    public ManagementBeanProvider( Class<?> beanInterface )
    {
        this( beanName( beanInterface ), beanInterface );
    }

    ManagementBeanProvider( String beanName )
    {
        this( beanName, DynamicMBean.class );
    }

    private ManagementBeanProvider( String beanName, Class<?> beanInterface )
    {
        super( beanName );
        this.beanName = beanName;
        this.beanInterface = beanInterface;
    }

    private static String beanName( Class<?> iface )
    {
        if ( iface.isInterface() )
        {
            try
            {
                return (String) iface.getField( "NAME" ).get( null );
            }
            catch ( Exception fallthrough )
            {
            }
        }
        throw new IllegalArgumentException( iface + " is not a Neo4j Management Been interface" );
    }

    protected abstract Neo4jMBean createMBean( KernelData kernel )
            throws NotCompliantMBeanException;

    protected Neo4jMBean createMXBean( KernelData kernel ) throws NotCompliantMBeanException
    {
        return createMBean( kernel );
    }

    Neo4jMBean loadBeen( KernelData kernel )
    {
        try
        {
            if ( BeanProxy.supportsMxBeans() )
            {
                return createMXBean( kernel );
            }
            else
            {
                return createMBean( kernel );
            }
        }
        catch ( Exception e )
        {
            return null;
        }
    }

    ObjectName getObjectName( KernelData kernel )
    {
        ObjectName name = JmxExtension.getObjectName( kernel, beanInterface, beanName );
        if ( name == null )
        {
            throw new IllegalArgumentException( beanInterface
                                                + " is not a Neo4j Management Bean interface" );
        }
        return name;
    }
}
