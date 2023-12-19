/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.kernel.impl.enterprise.transaction.log.checkpoint;

import org.neo4j.kernel.impl.transaction.log.checkpoint.AbstractCheckPointThreshold;

class ContinuousCheckPointThreshold extends AbstractCheckPointThreshold
{
    private volatile long nextTransactionIdTarget;

    ContinuousCheckPointThreshold()
    {
        super( "continuous threshold" );
    }

    @Override
    protected boolean thresholdReached( long lastCommittedTransactionId )
    {
        return lastCommittedTransactionId >= nextTransactionIdTarget;
    }

    @Override
    public void initialize( long transactionId )
    {
        checkPointHappened( transactionId );
    }

    @Override
    public void checkPointHappened( long transactionId )
    {
        nextTransactionIdTarget = transactionId + 1;
    }

    @Override
    public long checkFrequencyMillis()
    {
        return 100;
    }
}
