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
package org.neo4j.server.statistic;

import java.io.Serializable;

/**
 * @author tbaum
 * @since 31.05.11 20:00
 */
public class StatisticRecord implements Serializable
{

    private final long timeStamp;
    private final long period;
    private final long requests;
    private final StatisticData duration;
    private final StatisticData size;

    public StatisticRecord( long timeStamp, long period, long requests,
                            StatisticData duration, StatisticData size )
    {
        this.timeStamp = timeStamp;
        this.period = period;
        this.requests = requests;
        this.duration = duration;
        this.size = size;
    }

    public StatisticData getDuration()
    {
        return duration;
    }

    public long getPeriod()
    {
        return period;
    }

    public long getRequests()
    {
        return requests;
    }

    public StatisticData getSize()
    {
        return size;
    }

    public long getTimeStamp()
    {
        return timeStamp;
    }

    @Override
    public String toString()
    {
        return "StatisticRecord{" +
                "timeStamp=" + timeStamp +
                ", period=" + period +
                ", requests=" + requests +
                ", duration=" + duration +
                ", size=" + size +
                '}';
    }
}
