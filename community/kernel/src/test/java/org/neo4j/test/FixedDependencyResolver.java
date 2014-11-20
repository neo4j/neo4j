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
package org.neo4j.test;

import org.neo4j.graphdb.DependencyResolver;

/**
 * A stub DependencyResolver implementation that always returns the same dependency, no matter what is being asked for.
 */
public class FixedDependencyResolver implements DependencyResolver
{
    private final Object dependency;

    public FixedDependencyResolver( Object dependency )
    {
        this.dependency = dependency;
    }

    @Override
    public <T> T resolveDependency( Class<T> type ) throws IllegalArgumentException
    {
        return (T) dependency;
    }

    @Override
    public <T> T resolveDependency( Class<T> type, SelectionStrategy selector )
            throws IllegalArgumentException
    {
        return (T) dependency;
    }
}
