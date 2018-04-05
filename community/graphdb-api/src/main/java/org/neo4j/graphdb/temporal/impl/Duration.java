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
package org.neo4j.graphdb.temporal.impl;

public class Duration implements org.neo4j.graphdb.temporal.Duration
{
    private final long months;
    private final long days;
    private final long seconds;
    private final int nanos;

    private Duration( long months, long days, long seconds, long nanos )
    {
        seconds += nanos / NANOS_PER_SECOND;
        nanos %= NANOS_PER_SECOND;
        if ( seconds < 0 && nanos > 0 )
        {
            seconds += 1;
            nanos -= NANOS_PER_SECOND;
        }
        else if ( seconds > 0 && nanos < 0 )
        {
            seconds -= 1;
            nanos += NANOS_PER_SECOND;
        }
        this.months = months;
        this.days = days;
        this.seconds = seconds;
        this.nanos = (int) nanos;
    }

    @Override
    public long getMonths()
    {
        return months;
    }

    @Override
    public long getDays()
    {
        return days;
    }

    @Override
    public long getSeconds()
    {
        return seconds;
    }

    @Override
    public int getNanos()
    {
        return nanos;
    }
}
