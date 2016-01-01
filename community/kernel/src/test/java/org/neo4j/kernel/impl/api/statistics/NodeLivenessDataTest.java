/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.statistics;

import org.junit.Test;
import org.neo4j.kernel.impl.util.statistics.RollingAverage;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;

public class NodeLivenessDataTest
{
    @Test
    public void shouldReportOneIfNoDataAreSampled()
    {
        // given
        NodeLivenessData tracker = new NodeLivenessData( new RollingAverage.Parameters() );

        // when
        tracker.recalculate();

        // then
        assertThat( tracker.liveEntitiesRatio(), equalTo( 1.0 ) );
    }

    @Test
    public void shouldReportZeroLiveNodesIfItHasSeenNoLiveNodes()
    {
        // given
        NodeLivenessData tracker = new NodeLivenessData( new RollingAverage.Parameters() );

        // when
        tracker.recordDeadEntity();
        tracker.recalculate();

        // then
        assertThat( tracker.liveEntitiesRatio(), equalTo( 0.0 ) );
    }

    @Test
    public void shouldReportAPercentageOfLiveDeadNodeAccordinglyToTheObservedRecords()
    {
        // given
        NodeLivenessData tracker = new NodeLivenessData( new RollingAverage.Parameters() );

        // when
        tracker.recordLiveEntity();
        tracker.recordDeadEntity();
        tracker.recordLiveEntity();
        tracker.recalculate();

        // then
        assertThat( tracker.liveEntitiesRatio(), closeTo( 0.6666, 0.0001 ) );
    }

    @Test
    public void shouldReportOneIfOnlyLiveNodesAreRecorded()
    {
        // given
        NodeLivenessData tracker = new NodeLivenessData( new RollingAverage.Parameters() );

        // when
        tracker.recordLiveEntity();
        tracker.recalculate();

        // then
        assertThat( tracker.liveEntitiesRatio(), equalTo( 1.0 ) );
    }
}
