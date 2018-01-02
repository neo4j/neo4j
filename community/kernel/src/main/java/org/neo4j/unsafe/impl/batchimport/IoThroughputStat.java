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
package org.neo4j.unsafe.impl.batchimport;

import org.neo4j.helpers.Format;
import org.neo4j.unsafe.impl.batchimport.stats.DetailLevel;
import org.neo4j.unsafe.impl.batchimport.stats.Stat;

import static java.lang.System.currentTimeMillis;

/**
 * {@link Stat} that provides a simple Mb/s stat, mostly used for getting an insight into I/O throughput.
 */
public class IoThroughputStat implements Stat
{
    private final long startTime;
    private final long endTime;
    private final long position;

    public IoThroughputStat( long startTime, long endTime, long position )
    {
        this.startTime = startTime;
        this.endTime = endTime;
        this.position = position;
    }

    @Override
    public DetailLevel detailLevel()
    {
        return DetailLevel.IMPORTANT;
    }

    @Override
    public long asLong()
    {
        long endTime = this.endTime != 0 ? this.endTime : currentTimeMillis();
        long totalTime = endTime-startTime;
        int seconds = (int) (totalTime/1000);
        return seconds > 0 ? position/seconds : -1;
    }

    @Override
    public String toString()
    {
        long stat = asLong();
        return stat == -1 ? "??" : Format.bytes( stat ) + "/s";
    }
}
