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
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruning;

public class VolumetricCheckPointThreshold extends AbstractCheckPointThreshold
{
    private final LogPruning logPruning;

    public VolumetricCheckPointThreshold( LogPruning logPruning )
    {
        super( "tx log pruning" );
        this.logPruning = logPruning;
    }

    @Override
    protected boolean thresholdReached( long lastCommittedTransactionId )
    {
        return logPruning.mightHaveLogsToPrune();
    }

    @Override
    public void initialize( long transactionId )
    {
    }

    @Override
    public void checkPointHappened( long transactionId )
    {
    }

    @Override
    public long checkFrequencyMillis()
    {
        return DEFAULT_CHECKING_FREQUENCY_MILLIS;
    }
}
