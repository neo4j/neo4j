/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.metrics.source;

import java.net.InetSocketAddress;

import org.junit.Test;

import org.neo4j.metrics.source.causalclustering.MessageQueueMonitorMetric;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class MessageQueueMonitorMetricTest
{
    @Test
    public void shouldCalculateTotalDroppedMessages() throws Exception
    {
        // given
        MessageQueueMonitorMetric metric = new MessageQueueMonitorMetric();
        InetSocketAddress one = new InetSocketAddress( 1 );
        InetSocketAddress three = new InetSocketAddress( 3 );
        InetSocketAddress two = new InetSocketAddress( 2 );

        // when
        metric.register( one );
        metric.register( two );
        metric.register( three );

        metric.droppedMessage( one );
        metric.droppedMessage( two );
        metric.droppedMessage( three );

        // then
        assertThat( metric.droppedMessages(), equalTo( 3L ) );
    }

    @Test
    public void shouldCalculateTotalQueueSize() throws Exception
    {
        // given
        MessageQueueMonitorMetric metric = new MessageQueueMonitorMetric();
        InetSocketAddress one = new InetSocketAddress( 1 );
        InetSocketAddress two = new InetSocketAddress( 2 );
        InetSocketAddress three = new InetSocketAddress( 3 );

        // when
        metric.register( one );
        metric.register( two );
        metric.register( three );

        metric.queueSize( one, 5 );
        metric.queueSize( two, 6 );
        metric.queueSize( three, 7 );

        // then
        assertThat( metric.queueSizes(), equalTo(18L) );
    }
}
