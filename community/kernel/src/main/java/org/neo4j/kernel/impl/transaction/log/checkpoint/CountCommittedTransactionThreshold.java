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
package org.neo4j.kernel.impl.transaction.log.checkpoint;

class CountCommittedTransactionThreshold extends AbstractCheckPointThreshold
{
    private final int notificationThreshold;

    private volatile long nextTransactionIdTarget;

    CountCommittedTransactionThreshold( int notificationThreshold )
    {
        super( "tx count threshold" );
        this.notificationThreshold = notificationThreshold;
    }

    @Override
    public void initialize( long transactionId )
    {
        nextTransactionIdTarget = transactionId + notificationThreshold;
    }

    @Override
    protected boolean thresholdReached( long lastCommittedTransactionId )
    {
        return lastCommittedTransactionId >= nextTransactionIdTarget;
    }

    @Override
    public void checkPointHappened( long transactionId )
    {
        nextTransactionIdTarget = transactionId + notificationThreshold;
    }

    @Override
    public long checkFrequencyMillis()
    {
        // Transaction counts can change at any time, so we need to check fairly regularly to see if a checkpoint
        // should be triggered.
        return DEFAULT_CHECKING_FREQUENCY_MILLIS;
    }
}
