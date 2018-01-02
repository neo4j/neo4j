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
package org.neo4j.kernel.impl.util;

import org.neo4j.logging.Log;

import static java.lang.String.format;

public class DurationLogger implements AutoCloseable
{
    private final Log log;
    private final String tag;

    private long start = 0l;
    private long end = 0l;
    private String outcome = "Not finished";

    public DurationLogger( Log log, String tag )
    {
        this.log = log;
        this.tag = tag;
        start = System.currentTimeMillis();
        log.debug( format( "Started: %s", tag ) );
    }

    public void markAsFinished()
    {
        outcome = null;
    }

    public void markAsAborted( String cause )
    {
        outcome = format( "Aborted (cause: %s)", cause );
    }

    @Override
    public void close()
    {
        end = System.currentTimeMillis();
        long duration = end - start;
        if ( outcome == null )
        {
            log.debug( format( "Finished: %s in %d ms", tag, duration ) );
        }
        else
        {
            log.warn( format( "%s: %s in %d ms", outcome, tag, duration ) );
        }
    }
}
