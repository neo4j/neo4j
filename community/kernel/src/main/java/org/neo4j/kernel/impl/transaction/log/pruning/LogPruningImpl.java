/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.neo4j.kernel.impl.transaction.log.pruning.LogPruneStrategy.Monitor;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * This class listens for rotations and does log pruning.
 */
public class LogPruningImpl implements LogPruning
{
    private final Lock pruneLock = new ReentrantLock();
    private final LogPruneStrategy pruneStrategy;
    private final Log msgLog;
    private final Monitor monitor;

    public LogPruningImpl( LogPruneStrategy pruneStrategy, LogProvider logProvider )
    {
        this.pruneStrategy = pruneStrategy;
        this.msgLog = logProvider.getLog( getClass() );
        this.monitor = new LogPruneStrategy.Monitor()
        {
            @Override
            public void logsPruned( long upToVersion, long fromVersion, long toVersion )
            {
                msgLog.info( "Pruned log versions " + fromVersion + "-" + toVersion +
                        ", last checkpoint was made in version " + upToVersion );
            }

            @Override
            public void noLogsPruned( long upToVersion )
            {
                msgLog.info( "No log version pruned, last checkpoint was made in version " + upToVersion );
            }
        };
    }

    @Override
    public void pruneLogs( long upToVersion )
    {
        // Only one is allowed to do pruning at any given time,
        // and it's OK to skip pruning if another one is doing so right now.
        if ( pruneLock.tryLock() )
        {
            try
            {
                pruneStrategy.prune( upToVersion, monitor );
            }
            finally
            {
                pruneLock.unlock();
            }
        }
    }
}
