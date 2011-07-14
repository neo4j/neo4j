/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import java.util.Date;

public class RrdJob implements Job
{
    private static final long MIN_STEP_TIME = 1000;

    private RrdSampler sampler;
    private long lastRun = 0;

    public RrdJob( RrdSampler sampler )
    {
        this.sampler = sampler;
    }

    public void run()
    {
        // Guard against getting run in too rapid succession.
        if ( ( new Date().getTime() - lastRun ) >= MIN_STEP_TIME )
        {
        	lastRun = new Date().getTime();
            sampler.updateSample();
        }
    }
}
