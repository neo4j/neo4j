/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.util;

import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.multimap.list.MutableListMultimap;
import org.eclipse.collections.impl.factory.Multimaps;

import java.util.Objects;
import java.util.function.Supplier;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.collection.Iterables;

@SuppressWarnings( "unchecked" )
public class Dependencies extends DependencyResolver.Adapter implements DependencySatisfier
{
    private final DependencyResolver parent;
    private final MutableListMultimap<Class, Object> typeDependencies = Multimaps.mutable.list.empty();

    public Dependencies()
    {
        parent = null;
    }

    public Dependencies( DependencyResolver parent )
    {
        Objects.requireNonNull( parent );
        this.parent = parent;
    }

    @Override
    public <T> T resolveDependency( Class<T> type, SelectionStrategy selector )
    {
        RichIterable options = typeDependencies.get( type );
        if ( options.notEmpty() )
        {
            return selector.select( type, (Iterable<T>) options );
        }

        // Try parent
        if ( parent != null )
        {
            return parent.resolveDependency( type, selector );
        }

        // Out of options
        throw new UnsatisfiedDependencyException( type );
    }

    @Override
    public <T> Iterable<? extends T> resolveTypeDependencies( Class<T> type )
    {
        MutableList<T> options = (MutableList<T>) typeDependencies.get( type );
        if ( parent != null )
        {
            return Iterables.concat( options, parent.resolveTypeDependencies( type ) );
        }
        return options;
    }

    @Override
    public <T> Supplier<T> provideDependency( final Class<T> type, final SelectionStrategy selector )
    {
        return () -> resolveDependency( type, selector );
    }

    @Override
    public <T> Supplier<T> provideDependency( final Class<T> type )
    {
        return () -> resolveDependency( type );
    }

    @Override
    public <T> T satisfyDependency( T dependency )
    {
        // File this object under all its possible types
        Class<?> type = dependency.getClass();
        do
        {
            typeDependencies.put( type, dependency );

            // Add as all interfaces
            Class<?>[] interfaces = type.getInterfaces();
            addInterfaces(interfaces, dependency);

            type = type.getSuperclass();
        }
        while ( type != null );

        return dependency;
    }

    public void satisfyDependencies( Object... dependencies )
    {
        for ( Object dependency : dependencies )
        {
            satisfyDependency( dependency );
        }
    }

    private <T> void addInterfaces( Class<?>[] interfaces, T dependency )
    {
        for ( Class<?> type : interfaces )
        {
            typeDependencies.put( type, dependency );
            addInterfaces(type.getInterfaces(), dependency);
        }
    }
}
