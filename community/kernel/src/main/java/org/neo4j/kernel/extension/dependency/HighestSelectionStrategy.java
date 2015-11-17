/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.extension.dependency;

import java.util.Collections;
import java.util.List;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.extension.KernelExtensions;

import static org.neo4j.kernel.extension.KernelExtensionUtil.servicesClassPathEntryInformation;

/**
 * SelectionStrategy for {@link KernelExtensions kernel extensions loading} where the one with highest
 * natural order will be selected. If there are no such stores then an {@link IllegalStateException} will be
 * thrown.
 *
 * @see Comparable
 */
public class HighestSelectionStrategy implements DependencyResolver.SelectionStrategy
{
    private static final DependencyResolver.SelectionStrategy instance = new HighestSelectionStrategy();

    public static DependencyResolver.SelectionStrategy getInstance()
    {
        return instance;
    }

    private HighestSelectionStrategy()
    {
    }

    public <T> T select( Class<T> type, Iterable<T> candidates )
            throws IllegalArgumentException
    {
        List<Comparable> all = (List<Comparable>) Iterables.toList( candidates );
        if ( all.isEmpty() )
        {
            throw new IllegalArgumentException( "Could not resolve dependency of type: " +
                                                type.getName() + ". " + servicesClassPathEntryInformation() );
        }
        Collections.sort( all );
        return (T) all.get( all.size() - 1 );
    }
}
