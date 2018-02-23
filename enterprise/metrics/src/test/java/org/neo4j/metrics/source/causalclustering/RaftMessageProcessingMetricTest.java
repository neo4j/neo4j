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
package org.neo4j.metrics.source.causalclustering;

import com.codahale.metrics.SlidingWindowReservoir;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import org.neo4j.causalclustering.core.consensus.RaftMessages;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RaftMessageProcessingMetricTest
{
    private RaftMessageProcessingMetric metric = RaftMessageProcessingMetric.createUsing( () -> new SlidingWindowReservoir( 1000 ) );

    @Test
    void shouldDefaultAllMessageTypesToEmptyTimer()
    {
        for ( RaftMessages.Type type : RaftMessages.Type.values() )
        {
            assertEquals( 0, metric.timer( type ).getCount() );
        }
        assertEquals( 0, metric.timer().getCount() );
    }

    @Test
    void shouldBeAbleToUpdateAllMessageTypes()
    {
        // given
        int durationNanos = 5;
        double delta = 0.002;

        // when
        for ( RaftMessages.Type type : RaftMessages.Type.values() )
        {
            metric.updateTimer( type, Duration.ofNanos( durationNanos ) );
            assertEquals( 1, metric.timer( type ).getCount() );
            assertEquals( metric.timer( type ).getSnapshot().getMean(), delta, durationNanos );
        }

        // then
        assertEquals( RaftMessages.Type.values().length, metric.timer().getCount() );
        assertEquals( metric.timer().getSnapshot().getMean(), delta, durationNanos );
    }

    @Test
    void shouldDefaultDelayToZero()
    {
        assertEquals( 0, metric.delay() );
    }

    @Test
    void shouldUpdateDelay()
    {
        metric.setDelay( Duration.ofMillis( 5 ) );
        assertEquals( 5, metric.delay() );
    }
}
