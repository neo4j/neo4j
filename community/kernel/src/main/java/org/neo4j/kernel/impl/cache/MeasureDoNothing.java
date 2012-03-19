/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import org.neo4j.kernel.impl.util.StringLogger;

public class MeasureDoNothing extends Thread
{
    private volatile boolean measure = true;
    
    private volatile long timeBlocked = 0;
    
    private final long TIME_TO_WAIT;
    private final long TIME_BEFORE_BLOCK;
    private final StringLogger logger;
    
    public MeasureDoNothing( String threadName, StringLogger logger )
    {
        super( threadName );
        if ( logger == null )
        {
            throw new IllegalArgumentException( "Null message log" );
        }
        this.logger = logger;
        this.TIME_TO_WAIT = 100;
        this.TIME_BEFORE_BLOCK = 200;
        setDaemon( true );
    }
    
    public MeasureDoNothing( String threadName, StringLogger logger, long timeToWait, long acceptableWaitTime )
    {
        super( threadName );
        if ( logger == null )
        {
            throw new IllegalArgumentException( "Null message log" );
        }
        if ( timeToWait >= acceptableWaitTime )
        {
            throw new IllegalArgumentException( "timeToWait[" + timeToWait + "] should be less than acceptableWaitTime[" + acceptableWaitTime + "]" );
        }
        this.logger = logger;
        this.TIME_TO_WAIT = timeToWait;
        this.TIME_BEFORE_BLOCK = acceptableWaitTime;
        setDaemon( true );
    }
    
    @Override
    public synchronized void run()
    {
        logger.logMessage( "GC Monitor started. " );
        while ( measure )
        {
            long start = System.currentTimeMillis();
            try
            {
                this.wait( TIME_TO_WAIT );
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
            }
            long time = System.currentTimeMillis() - start;
            if ( time > TIME_BEFORE_BLOCK )
            {
                long blockTime = (time - TIME_TO_WAIT);
                timeBlocked += blockTime;
                logger.logMessage( "GC Monitor: Application threads blocked for an additional " + blockTime + 
                        "ms [total block time: " + (timeBlocked / 1000.0f) + "s]", true );
            }
        }
        logger.logMessage( "GC Monitor stopped. " );
    }
    
    public void stopMeasuring()
    {
        measure = false;
    }
    
    public long getTimeInBlock()
    {
        return timeBlocked;
    }
}