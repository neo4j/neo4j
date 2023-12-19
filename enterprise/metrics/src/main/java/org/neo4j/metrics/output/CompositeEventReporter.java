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
package org.neo4j.metrics.output;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;

/**
 * An implementation of the {@link EventReporter} interface that compounds several reporters internally.
 */
public class CompositeEventReporter implements EventReporter
{
    private final List<EventReporter> reporters = new ArrayList<>();

    @Override
    public void report( SortedMap<String,Gauge> gauges, SortedMap<String,Counter> counters,
            SortedMap<String,Histogram> histograms, SortedMap<String,Meter> meters, SortedMap<String,Timer> timers )
    {
        for ( EventReporter reporter : reporters )
        {
            reporter.report( gauges, counters, histograms, meters, timers );
        }
    }

    public void add( EventReporter reporter )
    {
        reporters.add( reporter );
    }

    public boolean isEmpty()
    {
        return reporters.isEmpty();
    }
}
