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

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class FirstValidRuleTest
{
    @Test
    public void shouldUseResultOfFirstNonEmpty()
    {
        // given
        Filter<Integer> removeValuesOfFive = data -> data.stream().filter( value -> value != 5 ).collect( Collectors.toSet() );
        Filter<Integer> countMoreThanFour = data -> data.size() > 4 ? data : Collections.emptySet();
        Filter<Integer> countMoreThanThree = data -> data.size() > 3 ? data : Collections.emptySet();

        FilterChain<Integer> ruleA = new FilterChain<>( asList( removeValuesOfFive, countMoreThanFour ) ); // should not succeed
        FilterChain<Integer> ruleB = new FilterChain<>( asList( removeValuesOfFive, countMoreThanThree ) ); // should succeed
        FilterChain<Integer> ruleC = new FilterChain<>( singletonList( countMoreThanFour ) ); // never reached

        FirstValidRule<Integer> firstValidRule = new FirstValidRule<>( asList( ruleA, ruleB, ruleC ) );

        Set<Integer> data = asSet( 5, 1, 5, 2, 5, 3, 5, 4 );

        // when
        data = firstValidRule.apply( data );

        // then
        assertEquals( asSet( 1, 2, 3, 4 ), data );
    }
}
