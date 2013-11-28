/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.util;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.lang.reflect.Proxy.newProxyInstance;

/**
 * This allows injecting monitoring listeners into a running component. It works by a component declaring a Monitor
 * interface, which contains methods that it will call at various interesting parts of it's regular operations. The
 * component asks for an implementation of the interface from this service, and uses that implementation to call
 * the various hook methods.
 *
 * An external stake holder can then register listeners through this monitor service, which will receive calls from the
 * original component as long as it is registered.
 */
public class Monitors
{
    @SuppressWarnings( "rawtypes" )
    private final ConcurrentMap<Class<?>, MonitorProxy> proxies = new ConcurrentHashMap<Class<?>, MonitorProxy>();
    private final StringLogger logger;

    public Monitors(StringLogger logger)
    {
        this.logger = logger;
    }

    /**
     * Get a new monitor instance, used by the component being monitored.
     */
    public <T> T newMonitor( Class<T> monitorInterface )
    {
        MonitorProxy<T> monitorProxy = proxyFor( monitorInterface );
        return monitorProxy.instance();
    }

    /**
     * Add a listener. The listener will receive calls for all valid monitor interfaces it implements.
     */
    public void addListener( Object listener )
    {
        for ( Class<?> monitorInterface : listener.getClass().getInterfaces() )
        {
            if( validMonitorInterface( monitorInterface ))
            {
                proxyFor( monitorInterface ).addListener( listener );
            }
        }
    }

    /**
     * Add a listener. The listener will receive calls for all valid monitor interfaces it implements.
     */
    public void removeListener( Object listener )
    {
        for ( Class<?> monitorInterface : listener.getClass().getInterfaces() )
        {
            if( validMonitorInterface( monitorInterface ))
            {
                proxyFor( monitorInterface ).removeListener( listener );
            }
        }
    }

    private static final class MonitorProxy<T> implements InvocationHandler
    {
        private final Class<T> monitorInterface;
        private final Set<T> listeners = new HashSet<T>();
        private final T proxyInstance;
        private final StringLogger logger;

        @SuppressWarnings( "unchecked" )
        private MonitorProxy(Class<T> monitorInterface, StringLogger logger)
        {
            this.monitorInterface = monitorInterface;
            this.logger = logger;
            assertValidMonitorInterface( monitorInterface );
            proxyInstance = (T) newProxyInstance( getClass().getClassLoader(), new Class[]{monitorInterface}, this );
        }

        public T instance()
        {
            return proxyInstance;
        }

        @Override
        public Object invoke( Object proxy, Method method, Object[] args ) throws Throwable
        {
            if ( !listeners.isEmpty() ) // at least saves instantiating an empty Iterator
            {
                for ( T listener : listeners )
                {
                    try
                    {
                        method.invoke( listener, args );
                    }
                    catch(Exception e)
                    {
                        logger.error( "Monitor listener failure.", e );
                    }
                }
            }

            return null;
        }

        public void addListener( Object rawListener )
        {
            listeners.add( monitorInterface.cast( rawListener ) );
        }

        public void removeListener( Object rawListener )
        {
            listeners.remove( monitorInterface.cast( rawListener ) );
        }
    }

    @SuppressWarnings( { "rawtypes", "unchecked" } )
    private <T> MonitorProxy<T> proxyFor( Class<T> monitorInterface )
    {
        if ( !proxies.containsKey( monitorInterface ) )
        {
            proxies.putIfAbsent( monitorInterface, new MonitorProxy( monitorInterface, logger ) );
        }
        return proxies.get( monitorInterface );
    }

    private static void assertValidMonitorInterface( Class<?> monitorInterface )
    {
        if ( !validMonitorInterface( monitorInterface ) )
        {
            throw new IllegalArgumentException( "Only void methods are allowed in monitor interfaces: "
                    + monitorInterface );
        }
    }

    private static boolean validMonitorInterface( Class<?> monitorInterface )
    {
        for ( Method method : monitorInterface.getDeclaredMethods() )
        {
            if ( method.getReturnType() != void.class )
            {
                return false;
            }
        }

        return true;
    }
}