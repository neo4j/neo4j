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

import java.util.Timer;
import java.util.TimerTask;

import org.neo4j.server.logging.Logger;

public class ScheduledJob
{
    private Job job;
    private Timer timer;
    private Logger logger = Logger.getLogger( ScheduledJob.class );

    public ScheduledJob( Job job, String name, int intervalInSeconds )
    {
        this.job = job;

        timer = new Timer( name );
        timer.scheduleAtFixedRate( runJob, 0, intervalInSeconds * 1000 );
    }

    private TimerTask runJob = new TimerTask()
    {
        public void run()
        {
            try
            {
                job.run();
            }
            catch ( Exception e )
            {
                logger.warn( e );
            }
        }
    };

    public void kill()
    {
        timer.cancel();
    }
}
