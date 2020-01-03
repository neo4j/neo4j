/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
    /** Monitor interface method -> Listeners */
    private final Map<Method,Set<MonitorListenerInvocationHandler>> methodMonitorListeners = new ConcurrentHashMap<>();
    private final MutableBag<Class<?>> monitoredInterfaces = MultiReaderHashBag.newBag();
    private final Monitors parent;

    public Monitors()
    {
        this( null );
    }

    /**
     * Create a child monitor with a given {@code parent}. Propagation works as expected where you can subscribe to
     * global monitors through the child monitor, but not the other way around. E.g. you can not subscribe to monitors
     * that are registered on the child monitor through the parent monitor.
     * <p>
     * Events will bubble up from the children in a way that listeners on the child monitor will be invoked before the
     * parent ones.
     *
     * @param parent to propagate events to and from.
     */
    public Monitors( Monitors parent )
    {
        this.parent = parent;
    }

    public <T> T newMonitor( Class<T> monitorClass, String... tags )
    {
        requireInterface( monitorClass );
        ClassLoader classLoader = monitorClass.getClassLoader();
        MonitorInvocationHandler monitorInvocationHandler = new MonitorInvocationHandler( this, tags );
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
        methodsStream( listenerInterfaces ).forEach( method -> cleanupMonitorListeners( monitorListener, method ) );
        listenerInterfaces.forEach( monitoredInterfaces::remove );
    }

    public boolean hasListeners( Class<?> monitorClass )
    {
        return monitoredInterfaces.contains( monitorClass ) || ((parent != null) && parent.hasListeners( monitorClass ));
    }

    private void cleanupMonitorListeners( Object monitorListener, Method key )
    {
        methodMonitorListeners.computeIfPresent( key, ( method1, handlers ) ->
        {
            handlers.removeIf( handler -> monitorListener.equals( handler.getMonitorListener() ) );
            return handlers.isEmpty() ? null : handlers;
        } );
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

    private static void requireInterface( Class monitorClass )
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

    private static class MonitorInvocationHandler implements InvocationHandler
    {
        private final Monitors monitor;
        private final String[] tags;

        MonitorInvocationHandler( Monitors monitor, String... tags )
        {
            this.monitor = monitor;
            this.tags = tags;
        }

        @Override
        public Object invoke( Object proxy, Method method, Object[] args )
        {
            invokeMonitorListeners( monitor, tags, proxy, method, args );

            // Bubble up
            Monitors current = monitor.parent;
            while ( current != null )
            {
                invokeMonitorListeners( current, tags, proxy, method, args );
                current = current.parent;
            }
            return null;
        }

        private static void invokeMonitorListeners( Monitors monitor, String[] tags, Object proxy, Method method, Object[] args )
        {
            Set<MonitorListenerInvocationHandler> handlers = monitor.methodMonitorListeners.get( method );
            if ( handlers == null || handlers.isEmpty() )
            {
                return;
            }
            for ( MonitorListenerInvocationHandler monitorListenerInvocationHandler : handlers )
            {
                try
                {
                    monitorListenerInvocationHandler.invoke( proxy, method, args, tags );
                }
                catch ( Throwable ignored )
                {
                }
            }
        }
    }
}
