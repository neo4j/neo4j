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
package org.neo4j.kernel.impl.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.function.Supplier;
import org.neo4j.graphdb.DependencyResolver;

@SuppressWarnings( "rawtypes" )
public class Dependencies extends DependencyResolver.Adapter implements DependencySatisfier
{
    private final Supplier<DependencyResolver> parent;
    private final Map<Class<?>, List<?>> typeDependencies = new HashMap<>();

    public Dependencies()
    {
        parent = null;
    }

    public Dependencies( final DependencyResolver parent )
    {
        this.parent = new Supplier<DependencyResolver>()
        {
            @Override
            public DependencyResolver get()
            {
                return parent;
            }
        };
    }

    public Dependencies( Supplier<DependencyResolver> parent )
    {
        this.parent = parent;
    }

    @Override
    public <T> T resolveDependency( Class<T> type, SelectionStrategy selector )
    {
        List<?> options = typeDependencies.get( type );

        if (options != null)
        {
            return selector.select( type, (Iterable<T>) options);
        }

        // Try parent
        if (parent != null)
        {
            DependencyResolver dependencyResolver = parent.get();

            if (dependencyResolver !=null)
                return dependencyResolver.resolveDependency( type, selector );
        }

        // Out of options
        throw new UnsatisfiedDependencyException( type );
    }

    public <T> Supplier<T> provideDependency( final Class<T> type, final SelectionStrategy selector)
    {
        return new Supplier<T>()
        {
            @Override
            public T get()
            {
                return resolveDependency( type, selector );
            }
        };
    }

    public <T> Supplier<T> provideDependency( final Class<T> type )
    {
        return new Supplier<T>()
        {
            @Override
            public T get()
            {
                return resolveDependency( type );
            }
        };
    }

    @Override
    public <T> T satisfyDependency( T dependency )
    {
        // File this object under all its possible types
        Class type = dependency.getClass();
        do
        {
            List<Object> deps = (List<Object>) typeDependencies.get( type );
            if (deps == null)
            {
                deps = new ArrayList<>(  );
                typeDependencies.put(type, deps);
            }
            deps.add( dependency );

            // Add as all interfaces
            Class[] interfaces = type.getInterfaces();
            addInterfaces(interfaces, dependency);

            type = type.getSuperclass();
        } while (type != null);

        return dependency;
    }

    public void satisfyDependencies(Object... dependencies)
    {
        for ( Object dependency : dependencies )
        {
            satisfyDependency( dependency );
        }
    }

    private <T> void addInterfaces( Class[] interfaces, T dependency )
    {
        for ( Class type : interfaces )
        {
            List<Object> deps = (List<Object>) typeDependencies.get( type );
            if (deps == null)
            {
                deps = new ArrayList<>(  );
                typeDependencies.put(type, deps);
            }
            deps.add( dependency );

            // Add as all sub-interfaces
            addInterfaces(type.getInterfaces(), dependency);
        }
    }
}
