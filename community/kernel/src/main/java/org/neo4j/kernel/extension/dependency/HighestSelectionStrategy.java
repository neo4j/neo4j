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
package org.neo4j.kernel.extension.dependency;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.neo4j.graphdb.DependencyResolver;
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
    public static final DependencyResolver.SelectionStrategy INSTANCE = new HighestSelectionStrategy();

    private HighestSelectionStrategy()
    {
    }

    @SuppressWarnings( "unchecked" )
    @Override
    public <T> T select( Class<T> type, Iterable<? extends T> candidates ) throws IllegalArgumentException
    {
        List<T> sorted = (List<T>) StreamSupport.stream( candidates.spliterator(), false )
                .map( ts -> (Comparable<T>) ts )
                .sorted()
                .collect( Collectors.toList() );

        if ( sorted.isEmpty() )
        {
            throw new IllegalArgumentException( "Could not resolve dependency of type: " +
                    type.getName() + ". " + servicesClassPathEntryInformation() );
        }
        return sorted.get( sorted.size() - 1 );
    }
}
