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
import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.Clock;
import org.neo4j.kernel.impl.transaction.log.IllegalLogFormatException;
import org.neo4j.kernel.impl.transaction.log.LogFileInformation;

public final class TransactionTimespanThreshold implements Threshold
{
    private final long timeToKeepInMillis;
    private final Clock clock;

    private long lowerLimit;

    TransactionTimespanThreshold( Clock clock, TimeUnit timeUnit, long timeToKeep )
    {
        this.clock = clock;
        this.timeToKeepInMillis = timeUnit.toMillis( timeToKeep );
    }

    @Override
    public void init()
    {
        lowerLimit = clock.currentTimeMillis() - timeToKeepInMillis;
    }

    @Override
    public boolean reached( File file, long version, LogFileInformation source )
    {
        try
        {
            long firstStartRecordTimestamp = source.getFirstStartRecordTimestamp( version );
            return firstStartRecordTimestamp >= 0 && firstStartRecordTimestamp < lowerLimit;
        }
        catch(IllegalLogFormatException e)
        {
            return LogPruneStrategyFactory.decidePruneForIllegalLogFormat( e );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}
