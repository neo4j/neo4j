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
package org.neo4j.metrics.source.causalclustering;

import com.codahale.metrics.SlidingWindowReservoir;
import org.junit.Test;

import java.time.Duration;

import org.neo4j.causalclustering.core.consensus.RaftMessages;

import static org.junit.Assert.assertEquals;

public class RaftMessageProcessingMetricTest
{
    private RaftMessageProcessingMetric metric = RaftMessageProcessingMetric.createUsing( () -> new SlidingWindowReservoir( 1000 ) );

    @Test
    public void shouldDefaultAllMessageTypesToEmptyTimer()
    {
        for ( RaftMessages.Type type : RaftMessages.Type.values() )
        {
            assertEquals( 0, metric.timer( type ).getCount() );
        }
        assertEquals( 0, metric.timer().getCount() );
    }

    @Test
    public void shouldBeAbleToUpdateAllMessageTypes()
    {
        // given
        int durationNanos = 5;
        double delta = 0.002;

        // when
        for ( RaftMessages.Type type : RaftMessages.Type.values() )
        {
            metric.updateTimer( type, Duration.ofNanos( durationNanos ) );
            assertEquals( 1, metric.timer( type ).getCount() );
            assertEquals( durationNanos, metric.timer( type ).getSnapshot().getMean(), delta );
        }

        // then
        assertEquals( RaftMessages.Type.values().length, metric.timer().getCount() );
        assertEquals( durationNanos, metric.timer().getSnapshot().getMean(), delta );
    }

    @Test
    public void shouldDefaultDelayToZero()
    {
        assertEquals( 0, metric.delay() );
    }

    @Test
    public void shouldUpdateDelay()
    {
        metric.setDelay( Duration.ofMillis( 5 ) );
        assertEquals( 5, metric.delay() );
    }
}
