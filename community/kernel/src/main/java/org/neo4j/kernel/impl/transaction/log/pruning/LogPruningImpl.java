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
package org.neo4j.kernel.impl.transaction.log.pruning;

import java.io.File;
import java.time.Clock;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongConsumer;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * This class listens for rotations and does log pruning.
 */
public class LogPruningImpl implements LogPruning
{
    private final Lock pruneLock = new ReentrantLock();
    private final FileSystemAbstraction fs;
    private final LogFiles logFiles;
    private final Log msgLog;
    private final LogPruneStrategyFactory strategyFactory;
    private final Clock clock;
    private volatile LogPruneStrategy pruneStrategy;

    public LogPruningImpl( FileSystemAbstraction fs,
                           LogFiles logFiles,
                           LogProvider logProvider,
                           LogPruneStrategyFactory strategyFactory,
                           Clock clock,
                           Config config )
    {
        this.fs = fs;
        this.logFiles = logFiles;
        this.msgLog = logProvider.getLog( getClass() );
        this.strategyFactory = strategyFactory;
        this.clock = clock;
        this.pruneStrategy = strategyFactory.strategyFromConfigValue( fs, logFiles, clock, config.get( GraphDatabaseSettings.keep_logical_logs ) );

        // Register listener for updates
        config.registerDynamicUpdateListener( GraphDatabaseSettings.keep_logical_logs,
                ( prev, update ) -> updateConfiguration( update ) );
    }

    private static class CountingDeleter implements LongConsumer
    {
        private static final int NO_VERSION = -1;
        private final LogFiles logFiles;
        private final FileSystemAbstraction fs;
        private final long upToVersion;
        private long fromVersion;
        private long toVersion;

        private CountingDeleter( LogFiles logFiles, FileSystemAbstraction fs, long upToVersion )
        {
            this.logFiles = logFiles;
            this.fs = fs;
            this.upToVersion = upToVersion;
            fromVersion = NO_VERSION;
            toVersion = NO_VERSION;
        }

        @Override
        public void accept( long version )
        {
            fromVersion = fromVersion == NO_VERSION ? version : Math.min( fromVersion, version );
            toVersion = toVersion == NO_VERSION ? version : Math.max( toVersion, version );
            File logFile = logFiles.getLogFileForVersion( version );
            fs.deleteFile( logFile );
        }

        public String describeResult()
        {
            if ( fromVersion == NO_VERSION )
            {
                return "No log version pruned, last checkpoint was made in version " + upToVersion;
            }
            else
            {
                return "Pruned log versions " + fromVersion + "-" + toVersion +
                       ", last checkpoint was made in version " + upToVersion;
            }
        }
    }

    private void updateConfiguration( String pruningConf )
    {
        this.pruneStrategy = strategyFactory.strategyFromConfigValue( fs, logFiles, clock, pruningConf );
        msgLog.info( "Retention policy updated, value will take effect during the next evaluation." );
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
                CountingDeleter deleter = new CountingDeleter( logFiles, fs, upToVersion );
                pruneStrategy.findLogVersionsToDelete( upToVersion ).forEachOrdered( deleter );
                msgLog.info( deleter.describeResult() );
            }
            finally
            {
                pruneLock.unlock();
            }
        }
    }

    @Override
    public boolean mightHaveLogsToPrune()
    {
        return pruneStrategy.findLogVersionsToDelete( logFiles.getHighestLogVersion() ).count() > 0;
    }
}
