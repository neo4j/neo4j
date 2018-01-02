/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
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
