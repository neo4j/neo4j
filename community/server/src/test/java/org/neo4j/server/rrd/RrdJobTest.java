/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.server.rrd;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.stub;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.Test;

public class RrdJobTest {

    @Test
    public void testGuardsAgainstQuickRuns() throws Exception {

        RrdSampler sampler = mock(RrdSampler.class);
        TimeSource time = mock(TimeSource.class);
        stub(time.getTime())
            .toReturn(10000l).toReturn(10000l) // First call (getTime gets called twice)
            .toReturn(10000l) // Second call
            .toReturn(12000l); // Third call

        RrdJob job = new RrdJob( time, sampler );

        job.run();
        job.run();
        job.run();

        verify(sampler, times(2)).updateSample();

    }

}
