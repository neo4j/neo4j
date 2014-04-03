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
package org.neo4j.kernel.impl.api.heuristics;

import org.junit.Test;
import org.neo4j.kernel.impl.util.statistics.RollingAverage;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;

public class NodeLivenessDataTest
{
    @Test
    public void shouldReportNonZeroLiveNodesWhenNotHavingSeenAnyLiveNodes()
    {
        // given
        NodeLivenessData tracker = new NodeLivenessData( new RollingAverage.Parameters() );

        // when
        tracker.recordDeadEntity();

        // then
        assertThat( tracker.liveEntitiesRatio(), not( equalTo( 0.0 ) ) );
    }
}
