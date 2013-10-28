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
package org.neo4j.kernel.extension;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.Listeners;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleException;
import org.neo4j.kernel.lifecycle.LifecycleListener;
import org.neo4j.kernel.lifecycle.LifecycleStatus;

import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.helpers.collection.Iterables.map;

public class KernelExtensions extends DependencyResolver.Adapter implements Lifecycle
{
    private final List<KernelExtensionFactory<?>> kernelExtensionFactories;
    private final DependencyResolver dependencyResolver;
    private final LifeSupport life = new LifeSupport();
    private final Map<Iterable<String>, Lifecycle> extensions = new HashMap<Iterable<String>, Lifecycle>();
    private Iterable<KernelExtensionListener> listeners = Listeners.newListeners();
    private final UnsatisfiedDependencyStrategy unsatisfiedDepencyStrategy;

    public KernelExtensions( Iterable<KernelExtensionFactory<?>> kernelExtensionFactories, Config config,
            DependencyResolver dependencyResolver, UnsatisfiedDependencyStrategy unsatisfiedDepencyStrategy )
    {
        this.unsatisfiedDepencyStrategy = unsatisfiedDepencyStrategy;
        this.kernelExtensionFactories = Iterables.addAll( new ArrayList<KernelExtensionFactory<?>>(),
                kernelExtensionFactories );
        this.dependencyResolver = dependencyResolver;

        life.addLifecycleListener( new LifecycleListener()
        {
            @Override
            public void notifyStatusChanged( final Object instance, LifecycleStatus from, LifecycleStatus to )
            {
                if ( to.equals( LifecycleStatus.STARTED ) )
                {
                    Listeners.notifyListeners( listeners, new Listeners.Notification<KernelExtensionListener>()
                    {
                        @Override
                        public void notify( KernelExtensionListener listener )
                        {
                            listener.startedKernelExtension( instance );
                        }
                    } );
                }
                else if ( to.equals( LifecycleStatus.STOPPING ) )
                {
                    Listeners.notifyListeners( listeners, new Listeners.Notification<KernelExtensionListener>()
                    {
                        @Override
                        public void notify( KernelExtensionListener listener )
                        {
                            listener.stoppingKernelExtension( instance );
                        }
                    } );
                }
            }
        } );
    }

    @Override
    public void init() throws Throwable
    {
        for ( KernelExtensionFactory kernelExtensionFactory : kernelExtensionFactories )
        {
            Object configuration = getKernelExtensionDependencies( kernelExtensionFactory );

            try
            {
                extensions.put( kernelExtensionFactory.getKeys(),
                        life.add( kernelExtensionFactory.newKernelExtension( configuration ) ) );
            }
            catch ( UnsatisfiedDepencyException e )
            {
                unsatisfiedDepencyStrategy.handle( kernelExtensionFactory, e );
            }
        }

        life.init();
    }

    @Override
    public void start() throws Throwable
    {
        life.start();
    }

    @Override
    public void stop() throws Throwable
    {
        life.stop();
    }

    @Override
    public void shutdown() throws Throwable
    {
        life.shutdown();
    }

    public boolean isRegistered( Class<?> kernelExtensionFactoryClass )
    {
        for ( KernelExtensionFactory<?> kernelExtensionFactory : kernelExtensionFactories )
        {
            if ( kernelExtensionFactoryClass.isInstance( kernelExtensionFactory ) )
            {
                return true;
            }
        }
        return false;
    }

    public synchronized void addKernelExtension( KernelExtensionFactory kernelExtensionFactory )
    {
        // Check that it is not already registered
        if ( kernelExtensionFactories.contains( kernelExtensionFactory ) )
        {
            return;
        }

        Lifecycle extension = null;
        try
        {
            extension = kernelExtensionFactory.newKernelExtension( getKernelExtensionDependencies(
                    kernelExtensionFactory ) );
            extensions.put( kernelExtensionFactory.getKeys(), extension );

            // Add to list of current factories
            kernelExtensionFactories.add( kernelExtensionFactory );
        }
        catch ( Throwable throwable )
        {
            throw new LifecycleException( extension, LifecycleStatus.NONE, LifecycleStatus.INITIALIZING, throwable );
        }

        life.add( extension );
    }

    public synchronized void removeKernelExtension( KernelExtensionFactory kernelExtensionFactory )
    {
        Lifecycle extension = extensions.remove( kernelExtensionFactory.getKeys() );
        if ( extension != null )
        {
            kernelExtensionFactories.remove( kernelExtensionFactory );
            life.remove( extension );
        }
    }

    public void addKernelExtensionListener( KernelExtensionListener listener )
    {
        listeners = Listeners.addListener( listener, listeners );

        // Notify listener about already started instances
        if ( life.getStatus().equals( LifecycleStatus.STARTED ) )
        {
            for ( Lifecycle extension : life.getLifecycleInstances() )
            {
                listener.startedKernelExtension( extension );
            }
        }
    }

    public void removeKernelExtensionListener( KernelExtensionListener listener )
    {
        listeners = Listeners.removeListener( listener, listeners );
    }

    @SuppressWarnings( { "unchecked", "rawtypes" } )
    @Override
    public <T> T resolveDependency( final Class<T> type, SelectionStrategy<T> selector ) throws IllegalArgumentException
    {
        Iterable<Lifecycle> lifecycleInstances = life.getLifecycleInstances();
        Iterable<Lifecycle> filteredInstances = filter( new TypeFilter( type ), lifecycleInstances );
        Iterable<T> mappedInstances = map( new CastFunction( type ), filteredInstances );
        return selector.select( type, mappedInstances );
    }

    private Object getKernelExtensionDependencies( KernelExtensionFactory<?> factory )
    {
        Class configurationClass = (Class) ((ParameterizedType) factory.getClass().getGenericSuperclass())
                .getActualTypeArguments()[0];
        return Proxy.newProxyInstance( configurationClass.getClassLoader(), new Class[]{configurationClass},
                new KernelExtensionHandler() );
    }

    public Iterable<KernelExtensionFactory<?>> listFactories()
    {
        return kernelExtensionFactories;
    }

    private static class TypeFilter<T> implements Predicate
    {
        private final Class<T> type;

        public TypeFilter( Class<T> type )
        {
            this.type = type;
        }

        @Override
        public boolean accept( Object extension )
        {
            return type.isInstance( extension );
        }
    }

    private class KernelExtensionHandler
            implements InvocationHandler
    {
        @Override
        public Object invoke( Object proxy, Method method, Object[] args ) throws Throwable
        {
            try
            {
                return dependencyResolver.resolveDependency( method.getReturnType() );
            }
            catch ( IllegalArgumentException e )
            {
                throw new UnsatisfiedDepencyException( e );
            }
        }
    }

    private class CastFunction<T> implements Function<Object, T>
    {
        private final Class<T> type;

        public CastFunction( Class<T> type )
        {
            this.type = type;
        }

        @Override
        public T apply( Object o )
        {
            return type.cast( o );
        }
    }
    
    static class UnsatisfiedDepencyException extends RuntimeException
    {
        public UnsatisfiedDepencyException( Throwable cause )
        {
            super( cause );
        }
    }
}
