/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.neo4j.function.Function;
import org.neo4j.function.Predicate;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.impl.util.DependenciesProxy;
import org.neo4j.kernel.impl.util.UnsatisfiedDependencyException;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;

import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.helpers.collection.Iterables.map;

public class KernelExtensions extends DependencyResolver.Adapter implements Lifecycle
{
    private final KernelContext kernelContext;
    private final List<KernelExtensionFactory<?>> kernelExtensionFactories;
    private final Dependencies dependencies;
    private final LifeSupport life = new LifeSupport();
    private final UnsatisfiedDependencyStrategy unsatisfiedDepencyStrategy;

    public KernelExtensions( KernelContext kernelContext, Iterable<KernelExtensionFactory<?>> kernelExtensionFactories,
                             Dependencies dependencies, UnsatisfiedDependencyStrategy unsatisfiedDependencyStrategy )
    {
        this.kernelContext = kernelContext;
        this.unsatisfiedDepencyStrategy = unsatisfiedDependencyStrategy;
        this.kernelExtensionFactories = Iterables.addAll( new ArrayList<KernelExtensionFactory<?>>(),
                kernelExtensionFactories );
        this.dependencies = dependencies;
    }

    @Override
    public void init() throws Throwable
    {

        for ( KernelExtensionFactory kernelExtensionFactory : kernelExtensionFactories )
        {
            Object kernelExtensionDependencies = getKernelExtensionDependencies( kernelExtensionFactory );

            try
            {
                Lifecycle dependency = kernelExtensionFactory.newInstance( kernelContext, kernelExtensionDependencies );
                Objects.requireNonNull( dependency, kernelExtensionFactory.toString() + " returned a null " +
                        "KernelExtension" );
                life.add( dependencies.satisfyDependency( dependency ) );
            }
            catch ( UnsatisfiedDependencyException e )
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

    @Override
    public <T> T resolveDependency( final Class<T> type, SelectionStrategy selector ) throws IllegalArgumentException
    {
        Iterable<Lifecycle> filtered = filter( new TypeFilter( type ), life.getLifecycleInstances() );
        Iterable<T> casted = map( new CastFunction( type ), filtered );
        return selector.select( type, casted );
    }

    private Object getKernelExtensionDependencies( KernelExtensionFactory<?> factory )
    {
        Class configurationClass = (Class) ((ParameterizedType) factory.getClass().getGenericSuperclass())
                .getActualTypeArguments()[0];
        return DependenciesProxy.dependencies(dependencies, configurationClass);
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
        public boolean test( Object extension )
        {
            return type.isInstance( extension );
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
}
