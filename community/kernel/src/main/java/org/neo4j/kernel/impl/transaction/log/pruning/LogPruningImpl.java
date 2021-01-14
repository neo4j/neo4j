/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.nio.file.Path;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongConsumer;

import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.time.SystemNanoClock;

import static org.apache.commons.lang3.ArrayUtils.isNotEmpty;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.checkpoint_logical_log_keep_threshold;
import static org.neo4j.configuration.GraphDatabaseSettings.keep_logical_logs;

/**
 * This class listens for rotations and does log pruning.
 */
public class LogPruningImpl implements LogPruning
{
    private final Lock pruneLock = new ReentrantLock();
    private final FileSystemAbstraction fs;
    private final LogFiles logFiles;
    private final Log log;
    private final LogPruneStrategyFactory strategyFactory;
    private final SystemNanoClock clock;
    private final LogProvider logProvider;
    private final int checkpointFilesToKeep;
    private volatile LogPruneStrategy pruneStrategy;

    public LogPruningImpl( FileSystemAbstraction fs,
                           LogFiles logFiles,
                           LogProvider logProvider,
                           LogPruneStrategyFactory strategyFactory,
                           SystemNanoClock clock,
                           Config config )
    {
        this.fs = fs;
        this.logFiles = logFiles;
        this.logProvider = logProvider;
        this.log = logProvider.getLog( getClass() );
        this.strategyFactory = strategyFactory;
        this.clock = clock;
        this.pruneStrategy = strategyFactory.strategyFromConfigValue( fs, logFiles, logProvider, clock, config.get( keep_logical_logs ) );
        this.checkpointFilesToKeep = config.get( checkpoint_logical_log_keep_threshold );

        // Register listener for updates
        config.addListener( keep_logical_logs, ( prev, update ) -> updateConfiguration( update ) );
    }

    private void updateConfiguration( String pruningConf )
    {
        LogPruneStrategy strategy = strategyFactory.strategyFromConfigValue( fs, logFiles, logProvider, clock, pruningConf );
        this.pruneStrategy = strategy;
        log.info( "Retention policy updated to '" + strategy + "', which will take effect next time a checkpoint completes." );
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
                CountingDeleter deleter = new CountingDeleter( logFiles, fs );
                LogPruneStrategy strategy = this.pruneStrategy;
                strategy.findLogVersionsToDelete( upToVersion ).forEachOrdered( deleter );
                log.info( deleter.describeResult( strategy ) );

                cleanupCheckpointLogFiles();
            }
            finally
            {
                pruneLock.unlock();
            }
        }
    }

    private void cleanupCheckpointLogFiles()
    {
        var checkpointFile = logFiles.getCheckpointFile();
        var checkpointFiles = checkpointFile.getDetachedCheckpointFiles();
        if ( isNotEmpty( checkpointFiles ) && checkpointFiles.length > checkpointFilesToKeep )
        {
            long highestVersionToRemove = checkpointFile.getCurrentDetachedLogVersion() - checkpointFilesToKeep;
            int filesDeleted = 0;
            for ( Path file : checkpointFiles )
            {
                if ( checkpointFile.getDetachedCheckpointLogFileVersion( file ) <= highestVersionToRemove )
                {
                    fs.deleteFile( file );
                    filesDeleted++;
                }
            }
            log.info( "Pruned " + filesDeleted + " checkpoint log files. Lowest preserved version: " + (highestVersionToRemove + 1) );
        }
    }

    @Override
    public boolean mightHaveLogsToPrune( long upToVersion )
    {
        return pruneStrategy.findLogVersionsToDelete( upToVersion ).count() > 0;
    }

    @Override
    public String describeCurrentStrategy()
    {
        return pruneStrategy.toString();
    }

    private static class CountingDeleter implements LongConsumer
    {
        private static final int NO_VERSION = -1;
        private final LogFiles logFiles;
        private final FileSystemAbstraction fs;
        private long fromVersion;
        private long toVersion;

        private CountingDeleter( LogFiles logFiles, FileSystemAbstraction fs )
        {
            this.logFiles = logFiles;
            this.fs = fs;
            fromVersion = NO_VERSION;
            toVersion = NO_VERSION;
        }

        @Override
        public void accept( long version )
        {
            fromVersion = fromVersion == NO_VERSION ? version : Math.min( fromVersion, version );
            toVersion = toVersion == NO_VERSION ? version : Math.max( toVersion, version );
            Path logFile = logFiles.getLogFile().getLogFileForVersion( version );
            fs.deleteFile( logFile );
        }

        String describeResult( LogPruneStrategy strategy )
        {
            String pruned = fromVersion == NO_VERSION ? "No log version pruned" :
                            fromVersion == toVersion ? "Pruned log version " + fromVersion :
                            "Pruned log versions " + fromVersion + " through " + toVersion;
            return pruned + ". The strategy used was '" + strategy + "'. ";
        }
    }
}
