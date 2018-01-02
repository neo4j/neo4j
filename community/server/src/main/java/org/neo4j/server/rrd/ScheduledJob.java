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

import java.util.Timer;
import java.util.TimerTask;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class ScheduledJob
{
    private final Timer timer;

    public ScheduledJob( final Runnable job, String name, long delay, long period, final LogProvider logProvider )
    {
        timer = new Timer( name );
        final Log log = logProvider.getLog( getClass() );

        TimerTask runJob = new TimerTask()
        {
            @Override
            public void run()
            {
                try
                {
                    job.run();
                } catch ( Exception e )
                {
                    log.warn( "Unable to execute scheduled job", e );
                }
            }
        };
        timer.scheduleAtFixedRate( runJob, delay, period );
    }

    public void cancel()
    {
        timer.cancel();
    }
}
