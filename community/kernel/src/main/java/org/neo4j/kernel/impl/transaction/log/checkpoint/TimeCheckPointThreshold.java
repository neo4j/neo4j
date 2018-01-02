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
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import org.neo4j.helpers.Clock;

public class TimeCheckPointThreshold extends AbstractCheckPointThreshold
{
    private volatile long lastCheckPointedTransactionId;
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
    public void initialize( long transactionId )
    {
        lastCheckPointedTransactionId = transactionId;
    }

    @Override
    protected boolean thresholdReached( long lastCommittedTransactionId )
    {
        return lastCommittedTransactionId > lastCheckPointedTransactionId &&
               clock.currentTimeMillis() >= nextCheckPointTime;
    }

    @Override
    protected String description()
    {
        return "time threshold";
    }

    @Override
    public void checkPointHappened( long transactionId )
    {
        nextCheckPointTime = clock.currentTimeMillis() + timeMillisThreshold;
        lastCheckPointedTransactionId = transactionId;
    }
}
