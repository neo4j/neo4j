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
package org.neo4j.collection;

import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.multimap.set.MutableSetMultimap;
import org.eclipse.collections.api.set.MutableSet;
import org.eclipse.collections.impl.factory.Multimaps;
import org.eclipse.collections.impl.factory.Sets;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import org.neo4j.common.DependencyResolver;
import org.neo4j.common.DependencySatisfier;
import org.neo4j.exceptions.UnsatisfiedDependencyException;

import static org.apache.commons.lang3.ClassUtils.getAllInterfaces;
import static org.apache.commons.lang3.ClassUtils.getAllSuperclasses;

@SuppressWarnings( "unchecked" )
public class Dependencies extends DependencyResolver.Adapter implements DependencySatisfier
{
    private final DependencyResolver parent;
    private final MutableSetMultimap<Class<?>, Object> typeDependencies = Multimaps.mutable.set.empty();

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
        RichIterable<Object> options = typeDependencies.get( type );
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
        MutableSet<T> options = (MutableSet<T>) typeDependencies.get( type );
        if ( parent != null )
        {
            options = Sets.mutable.ofAll( options );
            parent.resolveTypeDependencies( type ).forEach( options::add );
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
        typeDependencies.put( type, dependency );

        addSuperclasses( type, dependency );
        addInterfaces( type, dependency );

        return dependency;
    }

    private <T> void addInterfaces( Class<?> type, T dependency )
    {
        List<Class<?>> interfaces = getAllInterfaces( type );
        if ( interfaces != null )
        {
            interfaces.remove( type );
            for ( Class<?> iType : interfaces )
            {
                typeDependencies.put( iType, dependency );
            }
        }
    }

    private <T> void addSuperclasses( Class<?> type, T dependency )
    {
        List<Class<?>> allSuperclasses = getAllSuperclasses( type );
        if ( allSuperclasses != null )
        {
            for ( Class<?> aClass : allSuperclasses )
            {
                typeDependencies.put( aClass, dependency );
            }
        }
    }

    public void satisfyDependencies( Object... dependencies )
    {
        for ( Object dependency : dependencies )
        {
            satisfyDependency( dependency );
        }
    }
}
