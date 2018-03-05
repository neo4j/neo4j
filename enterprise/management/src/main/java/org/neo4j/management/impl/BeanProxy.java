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
