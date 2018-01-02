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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.LinkedList;

import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;

abstract class BeanProxy
{
    final boolean supportsMxBeans;

    private BeanProxy( boolean supportsMxBeans )
    {
        this.supportsMxBeans = supportsMxBeans;
    }

    public static <T> T load( MBeanServerConnection mbs, Class<T> beanInterface, ObjectName name )
    {
        return factory.makeProxy( mbs, beanInterface, name );
    }

    public static <T> Collection<T> loadAll( MBeanServerConnection mbs, Class<T> beanInterface, ObjectName query )
    {
        Collection<T> beans = new LinkedList<T>();
        try
        {
            for ( ObjectName name : mbs.queryNames( query, null ) )
            {
                beans.add( factory.makeProxy( mbs, beanInterface, name ) );
            }
        }
        catch ( IOException e )
        {
            // fall through and return the empty collection...
        }
        return beans;
    }

    static boolean supportsMxBeans()
    {
        return factory != null && factory.supportsMxBeans;
    }

    abstract <T> T makeProxy( MBeanServerConnection mbs, Class<T> beanInterface, ObjectName name );

    private static final BeanProxy factory;
    static
    {
        BeanProxy proxyMaker;
        try
        {
            proxyMaker = new Java6ProxyMaker();
        }
        catch ( Exception t )
        {
            proxyMaker = null;
        }
        catch ( LinkageError t )
        {
            proxyMaker = null;
        }
        if ( proxyMaker == null )
        {
            try
            {
                proxyMaker = new Java5ProxyMaker();
            }
            catch ( Exception t )
            {
                proxyMaker = null;
            }
            catch ( LinkageError t )
            {
                proxyMaker = null;
            }
        }
        factory = proxyMaker;
    }

    private static class Java6ProxyMaker extends BeanProxy
    {
        private final Method newMXBeanProxy;

        Java6ProxyMaker() throws Exception
        {
            super( true );
            Class<?> JMX = Class.forName( "javax.management.JMX" );
            this.newMXBeanProxy = JMX.getMethod( "newMXBeanProxy", MBeanServerConnection.class,
                    ObjectName.class, Class.class );
        }

        @Override
        <T> T makeProxy( MBeanServerConnection mbs, Class<T> beanType, ObjectName name )
        {
            try
            {
                return beanType.cast( newMXBeanProxy.invoke( null, mbs, name, beanType ) );
            }
            catch ( InvocationTargetException exception )
            {
                throw launderRuntimeException( exception.getTargetException() );
            }
            catch ( Exception exception )
            {
                throw new UnsupportedOperationException(
                        "Creating Management Bean proxies requires Java 1.6", exception );
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
    }

    private static class Java5ProxyMaker extends BeanProxy
    {
        Java5ProxyMaker() throws Exception
        {
            super( false );
            Class.forName( "javax.management.MBeanServerInvocationHandler" );
        }

        @Override
        <T> T makeProxy( MBeanServerConnection mbs, Class<T> beanType, ObjectName name )
        {
            return MBeanServerInvocationHandler.newProxyInstance( mbs, name, beanType, false );
        }
    }
}
