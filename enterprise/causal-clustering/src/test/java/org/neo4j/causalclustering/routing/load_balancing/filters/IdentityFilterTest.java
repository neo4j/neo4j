/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.routing.load_balancing.filters;

import org.junit.Test;

import java.util.Set;

import static java.util.Collections.unmodifiableSet;
import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class IdentityFilterTest
{
    @Test
    public void shouldNotFilter()
    {
        // given
        IdentityFilter<Object> identityFilter = IdentityFilter.as();

        // when
        Set<Object> input = unmodifiableSet( asSet( 1, 2, 3 ) );
        Set<Object> output = identityFilter.apply( input );

        // then
        assertEquals( input, output );
    }
}
