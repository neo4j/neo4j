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
package org.neo4j.metrics.output;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.TreeMap;
import java.util.function.LongConsumer;

import org.neo4j.helpers.HostnamePort;
import org.neo4j.logging.Log;

import static java.util.Collections.emptySortedMap;
import static org.junit.Assert.assertEquals;

public class PrometheusOutputTest
{
    @Test
    public void eventsShouldBeRedirectedToGauges()
    {
        String metricKey = "my.metric";
        MetricRegistry registry = new MetricRegistry();
        PrometheusOutput prometheusOutput =
                new PrometheusOutput( new HostnamePort( "localhost:8080" ), registry, Mockito.mock( Log.class ) );

        LongConsumer callback = durationMillis ->
        {
            TreeMap<String,Gauge> gauges = new TreeMap<>();
            gauges.put( metricKey, () -> durationMillis );
            prometheusOutput.report( gauges, emptySortedMap(), emptySortedMap(), emptySortedMap(), emptySortedMap() );
        };

        assertEquals( 0, registry.getGauges().size() );

        callback.accept( 10 );

        assertEquals( 1, registry.getGauges().size() );
        assertEquals( "10", registry.getGauges().get( metricKey ).getValue().toString() );

        callback.accept( 20 );

        assertEquals( 1, registry.getGauges().size() );
        assertEquals( "20", registry.getGauges().get( metricKey ).getValue().toString() );
    }
}
