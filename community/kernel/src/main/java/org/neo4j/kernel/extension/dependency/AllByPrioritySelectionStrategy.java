/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import static org.neo4j.kernel.extension.KernelExtensionUtil.servicesClassPathEntryInformation;

/**
 * Selects the candidate with highest priority (assumed to implement {@link Comparable}) and returns
 * in {@link #select(Class, Iterable)}, but keeps the others for access too.
 *
 * @param <T> type of items expected to be provided into {@link #select(Class, Iterable)}. Due to signature of the
 * {@link #select(Class, Iterable) select method} where an explicit and local {@code R} is defined a cast from
 * {@code R} to {@code T} is required and so will fail if {@code T} isn't matching {@code R}.
 */
public class AllByPrioritySelectionStrategy<T extends Comparable<T>> implements DependencyResolver.SelectionStrategy
{
    private List<T> lowerPrioritizedCandidates = Collections.emptyList();

    @SuppressWarnings( "unchecked" )
    @Override
    public <R> R select( Class<R> type, Iterable<? extends R> candidates ) throws IllegalArgumentException
    {
        List<T> all = (List<T>) Iterables.asList( candidates );
        if ( all.isEmpty() )
        {
            throw new IllegalArgumentException( "Could not resolve dependency of type: " +
                                                type.getName() + ". " + servicesClassPathEntryInformation() );
        }
        all.sort( Collections.reverseOrder() );
        R highest = (R) all.remove( 0 );
        lowerPrioritizedCandidates = all;
        return highest;
    }

    public Iterable<T> lowerPrioritizedCandidates()
    {
        return lowerPrioritizedCandidates;
    }
}
