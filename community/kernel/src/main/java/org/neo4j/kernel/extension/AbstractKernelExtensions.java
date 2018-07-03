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
package org.neo4j.kernel.extension;

import java.lang.reflect.ParameterizedType;
import java.util.List;
import java.util.Objects;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.impl.util.DependenciesProxy;
import org.neo4j.kernel.impl.util.UnsatisfiedDependencyException;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;

import static java.util.stream.Collectors.toList;
import static org.neo4j.helpers.collection.Iterables.stream;

public abstract class AbstractKernelExtensions extends DependencyResolver.Adapter implements Lifecycle
{
    private final KernelContext kernelContext;
    private final List<KernelExtensionFactory<?>> kernelExtensionFactories;
    private final Dependencies dependencies;
    private final LifeSupport life = new LifeSupport();
    private final UnsatisfiedDependencyStrategy unsatisfiedDependencyStrategy;

    AbstractKernelExtensions( KernelContext kernelContext, Iterable<KernelExtensionFactory<?>> kernelExtensionFactories, Dependencies dependencies,
            UnsatisfiedDependencyStrategy unsatisfiedDependencyStrategy, ExtensionType extensionType )
    {
        this.kernelContext = kernelContext;
        this.unsatisfiedDependencyStrategy = unsatisfiedDependencyStrategy;
        this.kernelExtensionFactories = stream( kernelExtensionFactories ).filter( e -> e.getExtensionType() == extensionType ).collect( toList() );
        this.dependencies = dependencies;
    }

    @Override
    public void init()
    {
        for ( KernelExtensionFactory<?> kernelExtensionFactory : kernelExtensionFactories )
        {
            Object kernelExtensionDependencies = getKernelExtensionDependencies( kernelExtensionFactory );
            try
            {
                Lifecycle dependency = newInstance( kernelContext, kernelExtensionFactory, kernelExtensionDependencies );
                Objects.requireNonNull( dependency, kernelExtensionFactory.toString() + " returned a null " +
                        "KernelExtension" );
                life.add( dependencies.satisfyDependency( dependency ) );
            }
            catch ( UnsatisfiedDependencyException e )
            {
                unsatisfiedDependencyStrategy.handle( kernelExtensionFactory, e );
            }
        }

        life.init();
    }

    @Override
    public void start()
    {
        life.start();
    }

    @Override
    public void stop()
    {
        life.stop();
    }

    @Override
    public void shutdown()
    {
        life.shutdown();
    }

    @Override
    public <T> T resolveDependency( final Class<T> type, SelectionStrategy selector ) throws IllegalArgumentException
    {
        List<T> filteredAndCasted = life.getLifecycleInstances().stream()
                .filter( type::isInstance )
                .map( type::cast )
                .collect( toList() );
        return selector.select( type, filteredAndCasted );
    }

    private Object getKernelExtensionDependencies( KernelExtensionFactory<?> factory )
    {
        Class<?> configurationClass = (Class<?>) ((ParameterizedType) factory.getClass().getGenericSuperclass())
                .getActualTypeArguments()[0];
        return DependenciesProxy.dependencies(dependencies, configurationClass);
    }

    @SuppressWarnings( "unchecked" )
    private static <T> Lifecycle newInstance( KernelContext kernelContext, KernelExtensionFactory<T> factory, Object dependencies )
    {
        return factory.newInstance( kernelContext, (T)dependencies );
    }
}
