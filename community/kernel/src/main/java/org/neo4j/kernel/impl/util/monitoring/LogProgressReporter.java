/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.util.monitoring;

import org.neo4j.logging.Log;

import static java.lang.String.format;

/**
 * Progress reporter that reports its progress into provided log.
 * Progress measured in percents (from 0 till 100) where just started reporter is at 0 percents and
 * completed is at 100. Progress reporter each 10 percents.
 */
public class LogProgressReporter implements ProgressReporter
{
    private static final int STRIDE = 10;
    private static final int HUNDRED = 100;

    private final Log log;

    private long current;
    private int currentPercent;
    private long max;

    public LogProgressReporter( Log log )
    {
        this.log = log;
    }

    @Override
    public void progress( long add )
    {
        current += add;
        int percent = max == 0 ? HUNDRED : Math.min( HUNDRED, (int) ((current * HUNDRED) / max) );
        ensurePercentReported( percent );
    }

    private void ensurePercentReported( int percent )
    {
        while ( currentPercent < percent )
        {
            reportPercent( ++currentPercent );
        }
    }

    private void reportPercent( int percent )
    {
        if ( percent % STRIDE == 0 )
        {
            log.info( format( "  %d%% completed", percent ) );
        }
    }

    @Override
    public void start( long max )
    {
        this.max = max;
    }

    @Override
    public void completed()
    {
        ensurePercentReported( HUNDRED );
    }
}
