/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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
import java.util.List;

import javax.management.MBeanServerConnection;
import javax.management.ObjectInstance;
import javax.management.ObjectName;

import org.neo4j.management.Kernel;

public abstract class KernelProxy
{
    protected final MBeanServerConnection server;
    protected final Kernel kernel;

    protected KernelProxy( MBeanServerConnection server, Kernel kernel )
    {
        this.server = server;
        this.kernel = kernel;
    }

    protected static ObjectName getObjectName( Class<?> beanType, String kernelIdentifier )
    {
        return JmxExtension.getObjectName( kernelIdentifier, beanType, null );
    }

    protected static <T> T proxy( MBeanServerConnection server, Class<T> beanType, ObjectName name )
    {
        return BeanProxy.load( server, beanType, name );
    }

    public List<Object> allBeans()
    {
        List<Object> beans = new ArrayList<Object>();
        Iterable<ObjectInstance> mbeans;
        try
        {
            mbeans = server.queryMBeans( kernel.getMBeanQuery(), null );
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
            catch ( Throwable e )
            {
            }
            if ( beanType != null && beanType != Kernel.class )
            {
                try
                {
                    beans.add( BeanProxy.load( server, beanType, instance.getObjectName() ) );
                }
                catch ( IllegalArgumentException couldNotCreateProxy )
                {
                }
            }
        }
        return beans;
    }

    protected ObjectName getObjectName( String beanName )
    {
        return JmxExtension.getObjectName( kernel.getMBeanQuery(), null, beanName );
    }

    protected ObjectName getObjectName( Class<?> beanInterface )
    {
        return JmxExtension.getObjectName( kernel.getMBeanQuery(), beanInterface, null );
    }

    protected <T> T getBean( Class<T> beanInterface )
    {
        return BeanProxy.load( server, beanInterface, getObjectName( beanInterface ) );
    }
}
