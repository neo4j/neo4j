/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import static org.neo4j.helpers.collection.Iterables.append;
import static org.neo4j.helpers.collection.Iterables.toArray;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.Iterables;

public class Monitors
{
    public interface Monitor
    {
        void monitorCreated( Class<?> monitorClass, String... tags );

        void monitorListenerException( Throwable throwable );

        public class Adapter
                implements Monitor
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

    private AtomicReference<Map<Method, List<MonitorListenerInvocationHandler>>> methodMonitorListeners = new
            AtomicReference<Map<Method, List<MonitorListenerInvocationHandler>>>( new HashMap<Method,
            List<MonitorListenerInvocationHandler>>() );
    private List<Class<?>> monitoredInterfaces = new ArrayList<Class<?>>();
    private Map<Predicate<Method>, MonitorListenerInvocationHandler> monitorListeners =
            new ConcurrentHashMap<Predicate<Method>, MonitorListenerInvocationHandler>();

    private Monitor monitorsMonitor;

    public Monitors()
    {
        monitorsMonitor = newMonitor( Monitor.class );
    }

    public <T> T newMonitor( Class<T> monitorClass, Class<?> owningClass, String... tags )
    {
        return newMonitor( monitorClass, toArray( String.class, append( owningClass.getName(), Iterables.<String, String>iterable(
                tags ) ) ) );
    }

    public <T> T newMonitor( Class<T> monitorClass, String... tags )
    {
        if ( !monitoredInterfaces.contains( monitorClass ) )
        {
            monitoredInterfaces.add( monitorClass );

            for ( Method method : monitorClass.getMethods() )
            {
                recalculateMethodListeners( method );
            }
        }

        ClassLoader classLoader = monitorClass.getClassLoader();
        MonitorInvocationHandler monitorInvocationHandler = new MonitorInvocationHandler( tags );
        try
        {
            return monitorClass.cast( Proxy.newProxyInstance( classLoader, new Class<?>[]{monitorClass},
                    monitorInvocationHandler ) );
        }
        finally
        {
            if ( monitorsMonitor != null )
            {
                monitorsMonitor.monitorCreated( monitorClass, tags );
            }
        }
    }

    public void addMonitorListener( final Object monitorListener, String... tags )
    {
        MonitorListenerInvocationHandler monitorListenerInvocationHandler = tags.length == 0 ? new
                UntaggedMonitorListenerInvocationHandler( monitorListener ) :
                new TaggedMonitorListenerInvocationHandler( monitorListener, tags );
        for ( Class<?> monitorInterface : getInterfacesOf( monitorListener.getClass() ) )
        {
            for ( final Method method : monitorInterface.getMethods() )
            {
                monitorListeners.put( new Predicate<Method>()
                {
                    @Override
                    public boolean accept( Method item )
                    {
                        return method.equals( item );
                    }
                }, monitorListenerInvocationHandler );

                recalculateMethodListeners( method );
            }
        }
    }

    public void removeMonitorListener( Object monitorListener )
    {
        Iterator<Map.Entry<Predicate<Method>, MonitorListenerInvocationHandler>> iter = monitorListeners.entrySet
                ().iterator();
        while ( iter.hasNext() )
        {
            Map.Entry<Predicate<Method>, MonitorListenerInvocationHandler> handlerEntry = iter.next();
            if ( handlerEntry.getValue() instanceof UntaggedMonitorListenerInvocationHandler )
            {
                if ( ((UntaggedMonitorListenerInvocationHandler)
                        handlerEntry.getValue()).getMonitorListener() == monitorListener )
                {
                    iter.remove();
                }
            }
        }

        recalculateAllMethodListeners();
    }

    public void addMonitorListener( MonitorListenerInvocationHandler invocationHandler,
                                    Predicate<Method> methodSpecification )
    {
        monitorListeners.put( methodSpecification, invocationHandler );

        recalculateAllMethodListeners();
    }

    public void removeMonitorListener( MonitorListenerInvocationHandler invocationHandler )
    {
        Iterator<Map.Entry<Predicate<Method>, MonitorListenerInvocationHandler>> iter = monitorListeners.entrySet
                ().iterator();
        while ( iter.hasNext() )
        {
            Map.Entry<Predicate<Method>, MonitorListenerInvocationHandler> handlerEntry = iter.next();
            if ( handlerEntry.getValue() == invocationHandler )
            {
                iter.remove();
                recalculateAllMethodListeners();
                return;
            }
        }
    }

    private void recalculateMethodListeners( Method method )
    {
        List<MonitorListenerInvocationHandler> listeners = new ArrayList<MonitorListenerInvocationHandler>();
        for ( Map.Entry<Predicate<Method>, MonitorListenerInvocationHandler> handlerEntry : monitorListeners
                .entrySet() )
        {
            if ( handlerEntry.getKey().accept( method ) )
            {
                listeners.add( handlerEntry.getValue() );
            }
        }
        methodMonitorListeners.get().put( method, listeners );
    }

    private void recalculateAllMethodListeners()
    {
        for ( Method method : methodMonitorListeners.get().keySet() )
        {
            recalculateMethodListeners( method );
        }
    }

    private Iterable<Class<?>> getInterfacesOf( Class<?> aClass )
    {
        List<Class<?>> interfaces = new ArrayList<Class<?>>();
        while ( aClass != null )
        {
            for ( Class<?> classInterface : aClass.getInterfaces() )
            {
                interfaces.add( classInterface );
            }
            aClass = aClass.getSuperclass();
        }
        return interfaces;
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
        private String[] tags;

        public TaggedMonitorListenerInvocationHandler( Object monitorListener, String... tags )
        {
            super( monitorListener );
            this.tags = tags;
        }

        @Override
        public void invoke( Object proxy, Method method, Object[] args, String... tags )
                throws Throwable
        {
            required:
            for ( int i = 0; i < this.tags.length; i++ )
            {
                String requiredTag = this.tags[i];
                for ( int j = 0; j < tags.length; j++ )
                {
                    String tag = tags[j];
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
            List<MonitorListenerInvocationHandler> handlers = methodMonitorListeners.get().get( method );
            if ( handlers != null )
            {
                for ( int i = 0; i < handlers.size(); i++ )
                {
                    MonitorListenerInvocationHandler monitorListenerInvocationHandler = handlers.get( i );
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
