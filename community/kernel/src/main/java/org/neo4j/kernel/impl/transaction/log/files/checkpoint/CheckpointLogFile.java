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
package org.neo4j.kernel.impl.transaction.log.files.checkpoint;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.neo4j.kernel.impl.transaction.log.LogEntryCursor;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckpointAppender;
import org.neo4j.kernel.impl.transaction.log.checkpoint.DetachedCheckpointAppender;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryDetachedCheckpoint;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogTailInformation;
import org.neo4j.kernel.impl.transaction.log.files.RangeLogVersionVisitor;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogChannelAllocator;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesContext;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper;
import org.neo4j.kernel.impl.transaction.log.rotation.monitor.LogRotationMonitor;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;

import static java.util.Collections.emptyList;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.checkpoint_logical_log_rotation_threshold;
import static org.neo4j.kernel.impl.transaction.log.LogVersionBridge.NO_MORE_CHANNELS;
import static org.neo4j.kernel.impl.transaction.log.entry.CheckpointLogVersionSelector.INSTANCE;
import static org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper.CHECKPOINT_FILE_PREFIX;
import static org.neo4j.kernel.impl.transaction.log.rotation.FileLogRotation.checkpointLogRotation;
import static org.neo4j.storageengine.api.CommandReaderFactory.NO_COMMANDS;

public class CheckpointLogFile extends LifecycleAdapter implements CheckpointFile
{
    private final DetachedCheckpointAppender checkpointAppender;
    private final DetachedLogTailScanner logTailScanner;
    private final TransactionLogFilesHelper fileHelper;
    private final TransactionLogChannelAllocator channelAllocator;
    private final TransactionLogFilesContext context;
    private final Log log;
    private final long rotationsSize;

    public CheckpointLogFile( LogFiles logFiles, TransactionLogFilesContext context )
    {
        this.context = context;
        this.rotationsSize = context.getConfig().get( checkpoint_logical_log_rotation_threshold );
        this.fileHelper = new TransactionLogFilesHelper( context.getFileSystem(), logFiles.logFilesDirectory(), CHECKPOINT_FILE_PREFIX );
        this.channelAllocator = new CheckpointLogChannelAllocator( context, fileHelper );
        this.logTailScanner = new DetachedLogTailScanner( logFiles, context, this );
        this.log = context.getLogProvider().getLog( getClass() );
        var rotationMonitor = context.getMonitors().newMonitor( LogRotationMonitor.class );
        var checkpointRotation = checkpointLogRotation( this, logFiles.getLogFile(), context.getClock(),
                context.getDatabaseHealth(), rotationMonitor );
        this.checkpointAppender = new DetachedCheckpointAppender( channelAllocator, context, this, checkpointRotation );
    }

    @Override
    public void start() throws Exception
    {
        checkpointAppender.start();
    }

    @Override
    public void shutdown() throws Exception
    {
        checkpointAppender.shutdown();
    }

    @Override
    public Optional<CheckpointInfo> findLatestCheckpoint() throws IOException
    {
        var versionVisitor = new RangeLogVersionVisitor();
        fileHelper.accept( versionVisitor );
        long highestVersion = versionVisitor.getHighestVersion();
        if ( highestVersion < 0 )
        {
            return Optional.empty();
        }

        long lowestVersion = versionVisitor.getLowestVersion();
        long currentVersion = highestVersion;

        var checkpointReader = new VersionAwareLogEntryReader( NO_COMMANDS, INSTANCE, true );
        while ( currentVersion >= lowestVersion )
        {
            try ( var channel = channelAllocator.openLogChannel( currentVersion );
                  var reader = new ReadAheadLogChannel( channel, NO_MORE_CHANNELS, context.getMemoryTracker() );
                  var logEntryCursor = new LogEntryCursor( checkpointReader, reader ) )
            {
                log.info( "Scanning log file with version %d for checkpoint entries", currentVersion );
                LogEntryDetachedCheckpoint checkpoint = null;
                var lastCheckpointLocation = reader.getCurrentPosition();
                var lastLocation = lastCheckpointLocation;
                while ( logEntryCursor.next() )
                {
                    lastCheckpointLocation = lastLocation;
                    LogEntry logEntry = logEntryCursor.get();
                    checkpoint = verify( logEntry );
                    lastLocation = reader.getCurrentPosition();
                }
                if ( checkpoint != null )
                {
                    return Optional.of( new CheckpointInfo( checkpoint, lastCheckpointLocation ) );
                }
                currentVersion--;
            }
        }
        return Optional.empty();
    }

    @Override
    public List<CheckpointInfo> reachableCheckpoints() throws IOException
    {
        var versionVisitor = new RangeLogVersionVisitor();
        fileHelper.accept( versionVisitor );
        long highestVersion = versionVisitor.getHighestVersion();
        if ( highestVersion < 0 )
        {
            return emptyList();
        }

        long currentVersion = versionVisitor.getLowestVersion();

        var checkpointReader = new VersionAwareLogEntryReader( NO_COMMANDS, INSTANCE, true );
        var checkpoints = new ArrayList<CheckpointInfo>();
        while ( currentVersion <= highestVersion )
        {
            try ( var channel = channelAllocator.openLogChannel( currentVersion );
                    var reader = new ReadAheadLogChannel( channel, NO_MORE_CHANNELS, context.getMemoryTracker() );
                    var logEntryCursor = new LogEntryCursor( checkpointReader, reader ) )
            {
                log.info( "Scanning log file with version %d for checkpoint entries", currentVersion );
                LogEntryDetachedCheckpoint checkpoint;
                var lastCheckpointLocation = reader.getCurrentPosition();
                var lastLocation = lastCheckpointLocation;
                while ( logEntryCursor.next() )
                {
                    lastCheckpointLocation = lastLocation;
                    LogEntry logEntry = logEntryCursor.get();
                    checkpoint = verify( logEntry );
                    checkpoints.add(  new CheckpointInfo( checkpoint, lastCheckpointLocation ) );
                    lastLocation = reader.getCurrentPosition();
                }
                currentVersion++;
            }
        }
        return checkpoints;
    }

    @Override
    public List<CheckpointInfo> getReachableDetachedCheckpoints() throws IOException
    {
        return reachableCheckpoints();
    }

    @Override
    public CheckpointAppender getCheckpointAppender()
    {
        return checkpointAppender;
    }

    @Override
    public LogTailInformation getTailInformation()
    {
        return logTailScanner.getTailInformation();
    }

    @Override
    public Path getCurrentFile()
    {
        return fileHelper.getLogFileForVersion( getCurrentDetachedLogVersion() );
    }

    @Override
    public Path getDetachedCheckpointFileForVersion( long logVersion )
    {
        return fileHelper.getLogFileForVersion( logVersion );
    }

    @Override
    public Path[] getDetachedCheckpointFiles()
    {
        return fileHelper.getMatchedFiles();
    }

    @Override
    public long getCurrentDetachedLogVersion()
    {
        var versionVisitor = new RangeLogVersionVisitor();
        fileHelper.accept( versionVisitor );
        return versionVisitor.getHighestVersion();
    }

    @Override
    public long getDetachedCheckpointLogFileVersion( Path checkpointLogFile )
    {
        return fileHelper.getLogVersion( checkpointLogFile );
    }

    private static LogEntryDetachedCheckpoint verify( LogEntry logEntry )
    {
        if ( logEntry instanceof LogEntryDetachedCheckpoint )
        {
            return (LogEntryDetachedCheckpoint) logEntry;
        }
        else
        {
            throw new UnsupportedOperationException( "Expected to observe only checkpoint entries, but: `" + logEntry + "` was found." );
        }
    }

    @Override
    public boolean rotationNeeded()
    {
        long position = checkpointAppender.getCurrentPosition();
        return position >= rotationsSize;
    }

    @Override
    public synchronized Path rotate() throws IOException
    {
        return checkpointAppender.rotate();
    }
}
