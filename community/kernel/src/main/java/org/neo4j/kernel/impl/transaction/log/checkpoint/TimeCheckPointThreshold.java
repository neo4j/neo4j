/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import org.neo4j.function.LongConsumer;
import org.neo4j.helpers.Clock;

public class TimeCheckPointThreshold implements CheckPointThreshold, LongConsumer
{
    private volatile boolean dirty = false;
    private volatile long nextCheckPointTime;

    private final long timeMillisThreshold;
    private final Clock clock;

    public TimeCheckPointThreshold( long timeMillisThreshold, Clock clock )
    {
        this.timeMillisThreshold = timeMillisThreshold;
        this.clock = clock;
        this.nextCheckPointTime = clock.currentTimeMillis() + timeMillisThreshold;

    }

    @Override
    public boolean isCheckPointingNeeded()
    {
        return dirty && clock.currentTimeMillis() >= nextCheckPointTime;
    }

    @Override
    public void checkPointHappened( long transactionId )
    {
        nextCheckPointTime = clock.currentTimeMillis() + timeMillisThreshold;
        dirty = false;
    }

    @Override
    public void accept( long transactionId )
    {
        dirty = true;
    }
}
