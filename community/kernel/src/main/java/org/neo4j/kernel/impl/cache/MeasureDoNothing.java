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
package org.neo4j.kernel.impl.cache;

import org.neo4j.logging.Log;

public class MeasureDoNothing extends Thread
{
    private volatile boolean measure = true;
    
    private final long TIME_TO_WAIT;
    private final long NOTIFICATION_THRESHOLD;
    private final Log log;
    
    public MeasureDoNothing( String threadName, Log log, long timeToWait, long pauseNotificationThreshold )
    {
        super( threadName );
        if ( log == null )
        {
            throw new IllegalArgumentException( "Null message log" );
        }
        this.log = log;
        this.TIME_TO_WAIT = timeToWait;
        this.NOTIFICATION_THRESHOLD = pauseNotificationThreshold + timeToWait;
        setDaemon( true );
    }
    
    @Override
    public synchronized void run()
    {
        log.debug( "GC Monitor started. " );
        while ( measure )
        {
            long start = System.nanoTime();
            try
            {
                this.wait( TIME_TO_WAIT );
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
            }
            long time = (System.nanoTime() - start) / 1_000_000;
            if ( time > NOTIFICATION_THRESHOLD )
            {
                long blockTime = time - TIME_TO_WAIT;
                log.warn( String.format( "GC Monitor: Application threads blocked for %dms.", blockTime ) );
            }
        }
        log.debug( "GC Monitor stopped. " );
    }
    
    public synchronized void stopMeasuring()
    {
        measure = false;
        this.interrupt();
    }
}
