/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.monitoring;

import org.apache.commons.lang3.ClassUtils;
import org.eclipse.collections.api.bag.MutableBag;
import org.eclipse.collections.impl.bag.mutable.MultiReaderHashBag;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import org.neo4j.helpers.ArrayUtil;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.apache.commons.lang3.ArrayUtils.isEmpty;

/**
 * This can be used to create monitor instances using a Dynamic Proxy, which when invoked can delegate to any number of
 * listeners. Listeners also implement the monitor interface.
 *
 * The creation of monitors and registration of listeners may happen in any order. Listeners can be registered before
 * creating the actual monitor, and vice versa.
 *
 * Components that actually implement listening functionality must be registered using {{@link #addMonitorListener(Object, String...)}.
 *
 * This class is thread-safe.
 */
public class Monitors
{
    // Concurrency: Mutation of these data structures is always guarded by the monitor lock on this Monitors instance,
    // while look-ups and reads are performed concurrently. The methodMonitorListeners lists (the map values) are
    // read concurrently by the proxies, while changing the listener set always produce new lists that atomically
    // replace the ones already in the methodMonitorListeners map.

    /** Monitor interface method -> Listeners */
    private final Map<Method,Set<MonitorListenerInvocationHandler>> methodMonitorListeners = new ConcurrentHashMap<>();
    private final MutableBag<Class<?>> monitoredInterfaces = MultiReaderHashBag.newBag();
    private final Log log;

    public Monitors()
    {
        this( NullLogProvider.getInstance() );
    }

    public Monitors( LogProvider logProvider )
    {
        this.log = logProvider.getLog( Monitors.class );
    }

    public <T> T newMonitor( Class<T> monitorClass, Class<?> owningClass, String... tags )
    {
        String[] monitorTags = ArrayUtil.concat( tags, owningClass.getName() );
        return newMonitor( monitorClass, monitorTags );
    }

    public <T> T newMonitor( Class<T> monitorClass, String... tags )
    {
        requireInterface( monitorClass );
        ClassLoader classLoader = monitorClass.getClassLoader();
        MonitorInvocationHandler monitorInvocationHandler = new MonitorInvocationHandler( tags );
        return monitorClass.cast( Proxy.newProxyInstance( classLoader, new Class<?>[]{monitorClass}, monitorInvocationHandler ) );
    }

    public void addMonitorListener( Object monitorListener, String... tags )
    {
        MonitorListenerInvocationHandler monitorListenerInvocationHandler = createInvocationHandler( monitorListener, tags );

        List<Class<?>> listenerInterfaces = getAllInterfaces( monitorListener );
        methodsStream( listenerInterfaces ).forEach( method ->
        {
            Set<MonitorListenerInvocationHandler> methodHandlers =
                    methodMonitorListeners.computeIfAbsent( method, f -> Collections.newSetFromMap( new ConcurrentHashMap<>() ) );
            methodHandlers.add( monitorListenerInvocationHandler );
        } );
        monitoredInterfaces.addAll( listenerInterfaces );
    }

    public void removeMonitorListener( Object monitorListener )
    {
        List<Class<?>> listenerInterfaces = getAllInterfaces( monitorListener );
        methodsStream( listenerInterfaces ).forEach( key -> methodMonitorListeners.computeIfPresent( key, ( method1, handlers ) ->
        {
            handlers.removeIf( handler -> monitorListener.equals( handler.getMonitorListener() ) );
            return handlers.isEmpty() ? null : handlers;
        } ) );
        listenerInterfaces.forEach( monitoredInterfaces::remove );
    }

    public boolean hasListeners( Class<?> monitorClass )
    {
        return monitoredInterfaces.contains( monitorClass );
    }

    private static List<Class<?>> getAllInterfaces( Object monitorListener )
    {
        return ClassUtils.getAllInterfaces( monitorListener.getClass() );
    }

    private static Stream<Method> methodsStream( List<Class<?>> interfaces )
    {
        return interfaces.stream().map( Class::getMethods ).flatMap( Arrays::stream );
    }

    private static MonitorListenerInvocationHandler createInvocationHandler( Object monitorListener, String[] tags )
    {
        return isEmpty( tags ) ? new UntaggedMonitorListenerInvocationHandler( monitorListener )
                               : new TaggedMonitorListenerInvocationHandler( monitorListener, tags );
    }

    private static <T> void requireInterface( Class<T> monitorClass )
    {
        if ( !monitorClass.isInterface() )
        {
            throw new IllegalArgumentException( "Interfaces should be provided." );
        }
    }
    private interface MonitorListenerInvocationHandler
    {

        Object getMonitorListener();

        void invoke( Object proxy, Method method, Object[] args, String... tags ) throws Throwable;
    }

    private static class UntaggedMonitorListenerInvocationHandler implements MonitorListenerInvocationHandler
    {
        private final Object monitorListener;

        UntaggedMonitorListenerInvocationHandler( Object monitorListener )
        {
            this.monitorListener = monitorListener;
        }

        @Override
        public Object getMonitorListener()
        {
            return monitorListener;
        }

        @Override
        public void invoke( Object proxy, Method method, Object[] args, String... tags ) throws Throwable
        {
            method.invoke( monitorListener, args );
        }
    }

    private static class TaggedMonitorListenerInvocationHandler extends UntaggedMonitorListenerInvocationHandler
    {
        private final String[] tags;

        TaggedMonitorListenerInvocationHandler( Object monitorListener, String... tags )
        {
            super( monitorListener );
            this.tags = tags;
        }

        @Override
        public void invoke( Object proxy, Method method, Object[] args, String... tags ) throws Throwable
        {
            if ( ArrayUtil.containsAll( this.tags, tags ) )
            {
                super.invoke( proxy, method, args, tags );
            }
        }
    }

    private class MonitorInvocationHandler implements InvocationHandler
    {
        private String[] tags;

        MonitorInvocationHandler( String... tags )
        {
            this.tags = tags;
        }

        @Override
        public Object invoke( Object proxy, Method method, Object[] args )
        {
            invokeMonitorListeners( proxy, method, args );
            return null;
        }

        private void invokeMonitorListeners( Object proxy, Method method, Object[] args )
        {
            Set<MonitorListenerInvocationHandler> handlers = methodMonitorListeners.get( method );
            if ( handlers != null )
            {
                for ( MonitorListenerInvocationHandler monitorListenerInvocationHandler : handlers )
                {
                    try
                    {
                        monitorListenerInvocationHandler.invoke( proxy, method, args, tags );
                    }
                    catch ( Throwable e )
                    {
                        String message = String.format( "Encountered exception while handling listener for monitor method %s", method.getName() );
                        log.warn( message, e );
                    }
                }
            }
        }
    }
}
