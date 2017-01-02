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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

import java.util.SortedMap;

public interface EventReporter
{
    /**
     * To be called whenever an event occurs. Note that this call might be blocking, hence don't call it from
     * time sensitive code or hot path code.
     *
     * @param gauges all of the gauges in the registry
     * @param counters all of the counters in the registry
     * @param histograms all of the histograms in the registry
     * @param meters all of the meters in the registry
     * @param timers all of the timers in the registry
     */
    void report( SortedMap<String,Gauge> gauges,
            SortedMap<String,Counter> counters,
            SortedMap<String,Histogram> histograms,
            SortedMap<String,Meter> meters,
            SortedMap<String,Timer> timers );
}
