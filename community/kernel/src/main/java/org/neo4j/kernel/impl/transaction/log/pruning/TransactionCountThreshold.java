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
package org.neo4j.kernel.impl.transaction.log.pruning;

import java.io.File;
import java.io.IOException;

import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.kernel.impl.transaction.log.IllegalLogFormatException;
import org.neo4j.kernel.impl.transaction.log.LogFileInformation;

public final class TransactionCountThreshold implements Threshold
{
    private final long maxTransactionCount;

    TransactionCountThreshold( long maxTransactionCount )
    {
        this.maxTransactionCount = maxTransactionCount;
    }

    @Override
    public void init()
    {
        // nothing to do here
    }

    @Override
    public boolean reached( File ignored, long version, LogFileInformation source )
    {
        try
        {
            // try to ask next version log file which is my last tx
            long lastTx = source.getFirstCommittedTxId( version + 1 );
            if ( lastTx == -1 )
            {
                throw new ThisShouldNotHappenError( "ChrisG", "The next version should always exist, since this is " +
                        "called after rotation and the PruneStrategy never checks the current active log file" );
            }

            long highest = source.getLastCommittedTxId();
            return highest - lastTx >= maxTransactionCount;
        }
        catch ( IllegalLogFormatException e )
        {
            return LogPruneStrategyFactory.decidePruneForIllegalLogFormat( e );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}
