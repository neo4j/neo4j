/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.monitoring;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import org.neo4j.helpers.collection.Iterables;

import static org.neo4j.helpers.collection.Iterables.append;
import static org.neo4j.helpers.collection.Iterables.asArray;

/**
 * This can be used to create monitor instances using a Dynamic Proxy, which when invoked can delegate to any number of
 * listeners. Listeners typically also implement the monitor interface, but it's possible to use a reflective style
 * to either do generic listeners, or avoid the performance penalty of Method.invoke().
 *
 * The creation of monitors and registration of listeners may happen in any order. Listeners can be registered before
 * creating the actual monitor, and vice versa.
 *
 * Typically only the top level component that creates Monitors should have a reference to it. When creating subcomponents
 * that uses monitors they should get instances of the monitor interface in the constructor, or if they need to create them
 * on demand then pass in a {@link org.neo4j.function.Factory} for that monitor instead. This allows tests to not have to use
 * Monitors, and instead can pass in mocks or similar.
 *
 * The other type of component that would have direct references to the Monitors instance are those that actually implement
 * listening functionality, and must call addMonitorListener.
 *
 * Monitors is monitorable itself, through the {@link org.neo4j.kernel.monitoring.Monitors.Monitor} monitor interface.
 *
 * This class, and the proxy objects it produces, are thread-safe.
 */
public class Monitors
{
    public static final AtomicBoolean FALSE = new AtomicBoolean(false);

    public interface Monitor
    {
        void monitorCreated( Class<?> monitorClass, String... tags );

        void monitorListenerException( Throwable throwable );

        class Adapter implements Monitor
        {
            @Override
            public void monitorCreated( Class<?> monitorClass, String... tags )
            {
            }

            @Override
            public void monitorListenerException( Throwable throwable )
            {
            }
        }
    }

    // Concurrency: Mutation of these data structures is always guarded by the monitor lock on this Monitors instance,
    // while look-ups and reads are performed concurrently. The methodMonitorListerners lists (the map values) are
    // read concurrently by the proxies, while changing the listener set always produce new lists that atomically
    // replace the ones already in the methodMonitorListeners map.
    /** Monitor interface method -> Listeners */
    private final Map<Method, List<MonitorListenerInvocationHandler>> methodMonitorListeners = new ConcurrentHashMap<>();

    /** Monitor interface -> Has Listeners? */
    private final Map<Class<?>,AtomicBoolean> monitoredInterfaces = new ConcurrentHashMap<>();

    /** Listener predicate -> Listener */
    private final Map<Predicate<Method>, MonitorListenerInvocationHandler> monitorListeners = new ConcurrentHashMap<>();

    private final Monitor monitorsMonitor;

    public Monitors()
    {
        monitorsMonitor = newMonitor( Monitor.class );
    }

    public synchronized <T> T newMonitor( Class<T> monitorClass, Class<?> owningClass, String... tags )
    {
        Iterable<String> tagIer = append( owningClass.getName(), Iterables.<String,String>iterable( tags ) );
        String[] tagArray = asArray( String.class, tagIer );
        return newMonitor( monitorClass, tagArray );
    }

    public synchronized <T> T newMonitor( Class<T> monitorClass, String... tags )
    {
        if ( !monitoredInterfaces.containsKey( monitorClass ) )
        {
            monitoredInterfaces.put( monitorClass, new AtomicBoolean(false) );

            for ( Method method : monitorClass.getMethods() )
            {
                recalculateMethodListeners( method );
            }
        }

        ClassLoader classLoader = monitorClass.getClassLoader();
        MonitorInvocationHandler monitorInvocationHandler = new MonitorInvocationHandler( tags );
        try
        {
            return monitorClass.cast( Proxy.newProxyInstance(
                    classLoader, new Class<?>[]{monitorClass}, monitorInvocationHandler ) );
        }
        finally
        {
            if ( monitorsMonitor != null )
            {
                monitorsMonitor.monitorCreated( monitorClass, tags );
            }
        }
    }

    public synchronized void addMonitorListener( final Object monitorListener, String... tags )
    {
        MonitorListenerInvocationHandler monitorListenerInvocationHandler =
                tags.length == 0 ? new UntaggedMonitorListenerInvocationHandler( monitorListener )
                                 : new TaggedMonitorListenerInvocationHandler( monitorListener, tags );

        for ( Class<?> monitorInterface : getInterfacesOf( monitorListener.getClass() ) )
        {
            for ( final Method method : monitorInterface.getMethods() )
            {
                monitorListeners.put(
                        Predicate.isEqual( method ),
                        monitorListenerInvocationHandler );

                recalculateMethodListeners( method );
            }
        }
    }

    public synchronized void removeMonitorListener( Object monitorListener )
    {
        Iterator<Map.Entry<Predicate<Method>, MonitorListenerInvocationHandler>> iter =
                monitorListeners.entrySet().iterator();

        while ( iter.hasNext() )
        {
            Map.Entry<Predicate<Method>, MonitorListenerInvocationHandler> handlerEntry = iter.next();
            if ( handlerEntry.getValue() instanceof UntaggedMonitorListenerInvocationHandler )
            {
                UntaggedMonitorListenerInvocationHandler handler =
                        (UntaggedMonitorListenerInvocationHandler) handlerEntry.getValue();

                if ( handler.getMonitorListener() == monitorListener )
                {
                    iter.remove();
                }
            }
        }

        recalculateAllMethodListeners();
    }

    // TODO: This is a *very* specialized API that adds sugar for monitoring a single
    //       method in a way that seems, from an end-user POV, would be much easier
    //       to do by just implementing the regular monitor interface, rather than
    //       dealing with reflection and predicates. It also seems to be used exclusively
    //       by tests. Maybe we can remove this? It seems it would simplify Monitors a lot.
    public synchronized void addMonitorListener(
            MonitorListenerInvocationHandler invocationHandler, Predicate<Method> methodSpecification )
    {
        monitorListeners.put( methodSpecification, invocationHandler );
        recalculateAllMethodListeners();
    }

    /**
     * While the intention is that the monitoring infrastructure itself should not
     * be a bottleneck (if it is, we should optimize it), components that use the
     * monitors may incur overhead in calculating whatever data they expose through
     * their monitors. If no-one is listening, this overhead is wasteful.
     *
     * This is a fast (single hash-map lookup) way to find out if there are
     * currently any listeners to a given monitor interface.
     */
    public boolean hasListeners( Class<?> monitorClass )
    {
        return monitoredInterfaces.getOrDefault( monitorClass, FALSE ).get();
    }

    private void recalculateMethodListeners( Method method )
    {
        Class<?> monitorClass = method.getDeclaringClass();
        List<MonitorListenerInvocationHandler> listeners = new ArrayList<>();
        for ( Map.Entry<Predicate<Method>, MonitorListenerInvocationHandler> handlerEntry : monitorListeners.entrySet() )
        {
            if ( handlerEntry.getKey().test( method ) )
            {
                listeners.add( handlerEntry.getValue() );
                markMonitorHasListener( monitorClass );
            }
        }
        methodMonitorListeners.put( method, listeners );
    }

    private void recalculateAllMethodListeners()
    {
        // Mark all monitored interfaces as having no listeners
        monitoredInterfaces.values().forEach( (b) -> b.set( false ) );
        for ( Method method : methodMonitorListeners.keySet() )
        {
            recalculateMethodListeners( method );
        }
    }

    private Iterable<Class<?>> getInterfacesOf( Class<?> aClass )
    {
        List<Class<?>> interfaces = new ArrayList<>();
        while ( aClass != null )
        {
            Collections.addAll( interfaces, aClass.getInterfaces() );
            aClass = aClass.getSuperclass();
        }
        return interfaces;
    }

    private void markMonitorHasListener( Class<?> monitorClass )
    {
        monitoredInterfaces.getOrDefault( monitorClass, new AtomicBoolean() )
                .set( true );
    }

    private static class UntaggedMonitorListenerInvocationHandler implements MonitorListenerInvocationHandler
    {
        private final Object monitorListener;

        public UntaggedMonitorListenerInvocationHandler( Object monitorListener )
        {
            this.monitorListener = monitorListener;
        }

        public Object getMonitorListener()
        {
            return monitorListener;
        }

        @Override
        public void invoke( Object proxy, Method method, Object[] args, String... tags )
                throws Throwable
        {
            method.invoke( monitorListener, args );
        }
    }

    private static class TaggedMonitorListenerInvocationHandler
            extends UntaggedMonitorListenerInvocationHandler
    {
        private final String[] tags;

        public TaggedMonitorListenerInvocationHandler( Object monitorListener, String... tags )
        {
            super( monitorListener );
            this.tags = tags;
        }

        @Override
        public void invoke( Object proxy, Method method, Object[] args, String... tags ) throws Throwable
        {
            required:
            for ( String requiredTag : this.tags )
            {
                for ( String tag : tags )
                {
                    if ( requiredTag.equals( tag ) )
                    {
                        continue required;
                    }
                }
                return; // Not all required tags present
            }

            super.invoke( proxy, method, args, tags );
        }
    }

    private class MonitorInvocationHandler implements InvocationHandler
    {
        private String[] tags;

        public MonitorInvocationHandler( String... tags )
        {
            this.tags = tags;
        }

        @Override
        public Object invoke( Object proxy, Method method, Object[] args ) throws Throwable
        {
            invokeMonitorListeners( proxy, method, args );
            return null;
        }

        private void invokeMonitorListeners( Object proxy, Method method, Object[] args )
        {
            List<MonitorListenerInvocationHandler> handlers = methodMonitorListeners.get( method );

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
                        if ( !method.getDeclaringClass().equals( Monitor.class ) )
                        {
                            monitorsMonitor.monitorListenerException( e );
                        }
                    }
                }
            }
        }
    }
}
