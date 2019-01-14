/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
