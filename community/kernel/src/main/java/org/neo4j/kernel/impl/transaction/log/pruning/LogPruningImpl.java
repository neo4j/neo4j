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

import java.io.File;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.LongStream;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * This class listens for rotations and does log pruning.
 */
public class LogPruningImpl implements LogPruning
{
    private final Lock pruneLock = new ReentrantLock();
    private final FileSystemAbstraction fs;
    private final LogPruneStrategy pruneStrategy;
    private final PhysicalLogFiles logFiles;
    private final Log msgLog;

    public LogPruningImpl( FileSystemAbstraction fs,
                           LogPruneStrategy pruneStrategy,
                           PhysicalLogFiles logFiles,
                           LogProvider logProvider )
    {
        this.fs = fs;
        this.pruneStrategy = pruneStrategy;
        this.logFiles = logFiles;
        this.msgLog = logProvider.getLog( getClass() );
    }

    private LongStream getLogVersionsToDelete( long upToVersion )
    {
        // We synchronise on the pruneStrategy because it's stateful and not thread safe, and even though we have the
        // pruneLock, we don't want mightHaveLogsToPrune to use it because it could make pruneLogs think that pruning
        // is taking place, when really we were just checking.
        // The pruneLock is used to guard against actual pruning happening concurrently, while synchronising on the
        // strategy is done to protect the internal state of the strategy.
        synchronized ( pruneStrategy )
        {
            return pruneStrategy.findLogVersionsToDelete( upToVersion );
        }
    }

    private void deleteLogVersion( long version )
    {
        File logFile = logFiles.getLogFileForVersion( version );
        fs.deleteFile( logFile );
    }

    @Override
    public void pruneLogs( long upToVersion )
    {
        // Only one is allowed to do pruning at any given time,
        // and it's OK to skip pruning if another one is doing so right now.
        if ( pruneLock.tryLock() )
        {
            String prefix = "Log Rotation [" + upToVersion + "]: ";
            msgLog.info( prefix + " Starting log pruning." );
            try
            {
                getLogVersionsToDelete( upToVersion ).forEachOrdered( this::deleteLogVersion );
            }
            finally
            {
                pruneLock.unlock();
                msgLog.info( prefix + " Log pruning complete." );
            }
        }
    }

    @Override
    public boolean mightHaveLogsToPrune()
    {
        return getLogVersionsToDelete( logFiles.getHighestLogVersion() ).count() > 0;
    }
}
