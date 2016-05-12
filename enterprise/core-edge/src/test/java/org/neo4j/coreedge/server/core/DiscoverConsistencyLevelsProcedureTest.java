/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.server.core;

import java.util.List;

import org.junit.Test;

import org.neo4j.collection.RawIterator;
import org.neo4j.kernel.api.exceptions.ProcedureException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import static org.neo4j.coreedge.server.core.EnterpriseCoreEditionModule.ConsistencyLevel.RYOW_CORE;
import static org.neo4j.coreedge.server.core.EnterpriseCoreEditionModule.ConsistencyLevel.RYOW_EDGE;
import static org.neo4j.helpers.collection.Iterators.asList;

public class DiscoverConsistencyLevelsProcedureTest
{
    @Test
    public void shouldRoundTripConsistencyLevelEnum() throws Exception
    {
        // given
        DiscoverConsistencyLevelsProcedure proc = new DiscoverConsistencyLevelsProcedure();

        // when
        final RawIterator<Object[], ProcedureException> rawIterator = proc.apply( null, new Object[0] );
        final List<Object[]> levels = asList( rawIterator );

        // then
        assertThat( levels, containsInAnyOrder(
                new Object[]{RYOW_CORE.name()},
                new Object[]{RYOW_EDGE.name()}
        ) );
    }
}
