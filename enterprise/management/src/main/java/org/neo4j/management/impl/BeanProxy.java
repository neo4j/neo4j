/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.management.impl;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.LinkedList;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

import org.neo4j.helpers.Exceptions;

public class BeanProxy
{
    private static final BeanProxy INSTANCE = new BeanProxy();

    public static <T> T load( MBeanServerConnection mbs, Class<T> beanInterface, ObjectName name )
    {
        return INSTANCE.makeProxy( mbs, beanInterface, name );
    }

    static <T> Collection<T> loadAll( MBeanServerConnection mbs, Class<T> beanInterface, ObjectName query )
    {
        Collection<T> beans = new LinkedList<>();
        try
        {
            for ( ObjectName name : mbs.queryNames( query, null ) )
            {
                beans.add( INSTANCE.makeProxy( mbs, beanInterface, name ) );
            }
        }
        catch ( IOException e )
        {
            // fall through and return the empty collection...
        }
        return beans;
    }

    private final Method newMXBeanProxy;

    private BeanProxy()
    {
        try
        {
            Class<?> jmx = Class.forName( "javax.management.JMX" );
            this.newMXBeanProxy = jmx.getMethod( "newMXBeanProxy", MBeanServerConnection.class, ObjectName.class, Class.class );
        }
        catch ( ClassNotFoundException | NoSuchMethodException e )
        {
            throw new RuntimeException( e );
        }
    }

    private <T> T makeProxy( MBeanServerConnection mbs, Class<T> beanInterface, ObjectName name )
    {
        try
        {
            return beanInterface.cast( newMXBeanProxy.invoke( null, mbs, name, beanInterface ) );
        }
        catch ( InvocationTargetException exception )
        {
            Exceptions.throwIfUnchecked( exception.getTargetException() );
            throw new RuntimeException( exception.getTargetException() );
        }
        catch ( Exception exception )
        {
            throw new UnsupportedOperationException(
                    "Creating Management Bean proxies requires Java 1.6", exception );
        }
    }
}
