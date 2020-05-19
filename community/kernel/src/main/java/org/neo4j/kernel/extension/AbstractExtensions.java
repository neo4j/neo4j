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
package org.neo4j.kernel.extension;

import java.lang.reflect.ParameterizedType;
import java.util.List;
import java.util.Objects;

import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.exceptions.UnsatisfiedDependencyException;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.impl.util.DependenciesProxy;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;

import static java.util.stream.Collectors.toList;
import static org.neo4j.internal.helpers.collection.Iterables.stream;

public abstract class AbstractExtensions extends DependencyResolver.Adapter implements Lifecycle
{
    private final ExtensionContext extensionContext;
    private final List<ExtensionFactory<?>> extensionFactories;
    private final Dependencies dependencies;
    private final LifeSupport life = new LifeSupport();
    private final ExtensionFailureStrategy extensionFailureStrategy;

    AbstractExtensions( ExtensionContext extensionContext, Iterable<ExtensionFactory<?>> extensionFactories, Dependencies dependencies,
            ExtensionFailureStrategy extensionFailureStrategy, ExtensionType extensionType )
    {
        this.extensionContext = extensionContext;
        this.extensionFailureStrategy = extensionFailureStrategy;
        this.extensionFactories = stream( extensionFactories ).filter( e -> e.getExtensionType() == extensionType ).collect( toList() );
        this.dependencies = dependencies;
    }

    @Override
    public void init()
    {
        for ( ExtensionFactory<?> extensionFactory : extensionFactories )
        {
            try
            {
                Object extensionDependencies = getExtensionDependencies( extensionFactory );
                Lifecycle dependency = newInstance( extensionContext, extensionFactory, extensionDependencies );
                Objects.requireNonNull( dependency, extensionFactory + " returned a null extension." );
                life.add( dependencies.satisfyDependency( dependency ) );
            }
            catch ( UnsatisfiedDependencyException exception )
            {
                extensionFailureStrategy.handle( extensionFactory, exception );
            }
            catch ( Throwable throwable )
            {
                extensionFailureStrategy.handle( extensionFactory, throwable );
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
    public <T> T resolveDependency( Class<T> type, SelectionStrategy selector )
    {
        Iterable<T> typeDependencies = resolveTypeDependencies( type );
        return selector.select( type, typeDependencies );
    }

    @Override
    public <T> Iterable<T> resolveTypeDependencies( Class<T> type )
    {
        return life.getLifecycleInstances().stream().filter( type::isInstance ).map( type::cast ).collect( toList() );
    }

    @Override
    public boolean containsDependency( Class<?> type )
    {
        return life.getLifecycleInstances().stream().anyMatch( type::isInstance );
    }

    private Object getExtensionDependencies( ExtensionFactory<?> factory )
    {
        Class<?> factoryType = factory.getClass();
        while ( !(factoryType.getGenericSuperclass() instanceof ParameterizedType) )
        {
            factoryType = factoryType.getSuperclass();
        }
        Class<?> configurationClass = (Class<?>) ((ParameterizedType) factoryType.getGenericSuperclass()).getActualTypeArguments()[0];
        return DependenciesProxy.dependencies(dependencies, configurationClass);
    }

    @SuppressWarnings( "unchecked" )
    private static <T> Lifecycle newInstance( ExtensionContext extensionContext, ExtensionFactory<T> factory, Object dependencies )
    {
        return factory.newInstance( extensionContext, (T)dependencies );
    }
}
