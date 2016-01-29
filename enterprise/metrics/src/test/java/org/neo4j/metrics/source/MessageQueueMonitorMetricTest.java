package org.neo4j.metrics.source;

import java.net.InetSocketAddress;

import org.junit.Test;

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