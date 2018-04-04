/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.routing.load_balancing.filters;

import org.junit.Test;

import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class FilterChainTest
{
    @Test
    public void shouldFilterThroughAll()
    {
        // given
        Filter<Integer> removeValuesOfFive = data -> data.stream().filter( value -> value != 5 ).collect( Collectors.toSet() );
        Filter<Integer> mustHaveThreeValues = data -> data.size() == 3 ? data : emptySet();
        Filter<Integer> keepValuesBelowTen = data -> data.stream().filter( value -> value < 10 ).collect( Collectors.toSet() );

        FilterChain<Integer> filterChain = new FilterChain<>( asList( removeValuesOfFive, mustHaveThreeValues, keepValuesBelowTen ) );
        Set<Integer> data = asSet( 5, 5, 5, 3, 5, 10, 9 ); // carefully crafted to check order as well

        // when
        data = filterChain.apply( data );

        // then
        assertEquals( asSet( 3, 9 ), data );
    }
}
