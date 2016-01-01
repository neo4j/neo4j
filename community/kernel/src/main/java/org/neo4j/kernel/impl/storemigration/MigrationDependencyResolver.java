/**
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
package org.neo4j.kernel.impl.storemigration;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.Function;
import org.neo4j.kernel.impl.util.DependencySatisfier;
import org.neo4j.kernel.impl.util.UnsatisfiedDependencyException;

@SuppressWarnings( "rawtypes" )
public class MigrationDependencyResolver extends DependencyResolver.Adapter implements DependencySatisfier
{
    private final Map<Class, Object> dependencies = new HashMap<>();

    @Override
    public <T> T resolveDependency( Class<T> type, SelectionStrategy selector )
            throws UnsatisfiedDependencyException
    {
        // Try super classes
        Object dependency = dependencies.get( type );
        if ( dependency == null )
        {
            dependency = getDependencyForType( type, SUPER_CLASS );
        }

        // Try interfaces
        if ( dependency == null )
        {
            dependency = getDependencyForType( type, INTERFACES );
        }

        // Out of options
        if ( dependency == null )
        {
            throw new UnsatisfiedDependencyException( new Exception(
                    "Weird exception nesting here, but anyways, I couldn't find any dependency for " + type ) );
        }

        // We found it
        return type.cast( dependency );
    }

    @Override
    public <T> void satisfyDependency( Class<T> type, T dependency )
    {
        this.dependencies.put( type, dependency );
    }

    private Object getDependencyForType( Class type, Function<Class,Class[]> traverser )
    {
        for ( Class candidate : traverser.apply( type ) )
        {
            Object dependency = dependencies.get( candidate );
            if ( dependency != null )
            {
                return dependency;
            }

            // Recursive call here
            return getDependencyForType( candidate, traverser );
        }
        return null;
    }

    private static final Function<Class,Class[]> SUPER_CLASS = new Function<Class,Class[]>()
    {
        @Override
        public Class[] apply( Class from )
        {
            Class superClass = from.getSuperclass();
            if ( superClass == null )
            {
                return NO_CLASSES;
            }
            return new Class[] { superClass };
        }
    };

    private static final Function<Class,Class[]> INTERFACES = new Function<Class,Class[]>()
    {

        @Override
        public Class[] apply( Class from )
        {
            return from.getInterfaces();
        }
    };

    private static final Class[] NO_CLASSES = new Class[0];
}
