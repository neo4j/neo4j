/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.graphdb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Find a dependency given a type. This can be the exact type or a super type of
 * the actual dependency.
 */
public interface DependencyResolver
{
    /**
     * Tries to resolve a dependency that matches a given class. No specific
     * {@link SelectionStrategy} is used, so the first encountered matching dependency will be returned.
     *
     * @param type the type of {@link Class} that the returned instance must implement.
     * @return the resolved dependency for the given type.
     * @throws IllegalArgumentException if no matching dependency was found.
     */
    <T> T resolveDependency( Class<T> type ) throws IllegalArgumentException;

    /**
     * Tries to resolve a dependency that matches a given class. All candidates are fed to the
     * {@code selector} which ultimately becomes responsible for making the choice between all available candidates.
     *
     * @param type the type of {@link Class} that the returned instance must implement.
     * @param selector {@link SelectionStrategy} which will make the choice of which one to return among
     * matching candidates.
     * @return the resolved dependency for the given type.
     * @throws IllegalArgumentException if no matching dependency was found.
     */
    <T> T resolveDependency( Class<T> type, SelectionStrategy selector ) throws IllegalArgumentException;

    /**
     * Responsible for making the choice between available candidates.
     */
    interface SelectionStrategy
    {
        /**
         * Given a set of candidates, select an appropriate one. Even if there are candidates this
         * method may throw {@link IllegalArgumentException} if there was no suitable candidate.
         *
         * @param type the type of items.
         * @param candidates candidates up for selection, where one should be picked. There might
         * also be no suitable candidate, in which case an exception should be thrown.
         * @return a suitable candidate among all available.
         * @throws IllegalArgumentException if no suitable candidate was found.
         */
        <T> T select( Class<T> type, Iterable<T> candidates ) throws IllegalArgumentException;
    }

    /**
     * Adapter for {@link DependencyResolver} which will select the first available candidate by default
     * for {@link #resolveDependency(Class)}.
     */
    abstract class Adapter implements DependencyResolver
    {
        private static final SelectionStrategy FIRST = new SelectionStrategy()
        {
            @Override
            public <T> T select( Class<T> type, Iterable<T> candidates ) throws IllegalArgumentException
            {
                Iterator<T> iterator = candidates.iterator();
                if ( !iterator.hasNext() )
                {
                    throw new IllegalArgumentException( "Could not resolve dependency of type:" + type.getName() );
                }
                return iterator.next();
            }
        };

        @Override
        public <T> T resolveDependency( Class<T> type ) throws IllegalArgumentException
        {
            return resolveDependency( type, FIRST );
        }
    }

    class Dependencies
        extends Adapter
    {
        List<Object> dependencies = new ArrayList<>(  );

        public <T> T add(T instance)
        {
            dependencies.add(instance);
            return instance;
        }

        public boolean remove(Object instance)
        {
            return dependencies.remove( instance );
        }

        public void clear()
        {
            dependencies.clear();
        }

        @Override
        public <T> T resolveDependency( Class<T> type, SelectionStrategy selector ) throws IllegalArgumentException
        {
            List<T> candidates = new ArrayList<>(  );
            for ( Object dependency : dependencies )
            {
                if (type.isInstance( dependency ))
                    candidates.add(type.cast(dependency));
            }

            return selector.select( type, candidates );
        }
    }
}
