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
package org.neo4j.kernel.impl.transaction.log.pruning;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation.PrintFormat;

/**
 * This class listens for rotations and does log pruning.
 */
public class LogPruning
    implements LogRotation.Monitor
{
    private final Lock pruneLock = new ReentrantLock();
    private final LogPruneStrategy pruneStrategy;
    private final Log msgLog;

    public LogPruning( LogPruneStrategy pruneStrategy, LogProvider logProvider )
    {
        this.pruneStrategy = pruneStrategy;
        msgLog = logProvider.getLog( getClass() );
    }

    @Override
    public void startedRotating( long currentVersion )
    {
    }

    @Override
    public void finishedRotating( long currentVersion )
    {
        // Only one is allowed to do pruning at any given time,
        // and it's OK to skip pruning if another one is doing so right now.
        if ( pruneLock.tryLock() )
        {
            Thread thread = Thread.currentThread();
            String threadStr = "[" + thread.getId() + ":" + thread.getName() + "]";

            msgLog.info( PrintFormat.prefix( currentVersion ) + threadStr + " Starting log pruning." );

            try
            {
                pruneStrategy.prune();
            }
            finally
            {
                pruneLock.unlock();
            }

            msgLog.info( PrintFormat.prefix( currentVersion ) + threadStr + " Log pruning complete." );
        }
    }
}
