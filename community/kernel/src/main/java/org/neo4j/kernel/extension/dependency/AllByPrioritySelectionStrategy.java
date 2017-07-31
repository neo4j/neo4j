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
 */
public class AllByPrioritySelectionStrategy<T> implements DependencyResolver.SelectionStrategy
{
    @SuppressWarnings( "rawtypes" )
    private List lowerPrioritizedCandidates = Collections.emptyList();

    @SuppressWarnings( {"rawtypes", "unchecked"} )
    @Override
    public <R> R select( Class<R> type, Iterable<R> candidates ) throws IllegalArgumentException
    {
        List<Comparable> all = (List<Comparable>) Iterables.asList( candidates );
        if ( all.isEmpty() )
        {
            throw new IllegalArgumentException( "Could not resolve dependency of type: " +
                                                type.getName() + ". " + servicesClassPathEntryInformation() );
        }
        Collections.sort( all, Collections.reverseOrder() );
        R highest = (R) all.remove( 0 );
        lowerPrioritizedCandidates = all;
        return highest;
    }

    @SuppressWarnings( "unchecked" )
    public Iterable<T> lowerPrioritizedCandidates()
    {
        return lowerPrioritizedCandidates;
    }
}
