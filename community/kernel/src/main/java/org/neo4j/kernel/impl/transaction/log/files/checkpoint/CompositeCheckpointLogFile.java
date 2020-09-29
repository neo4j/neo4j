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
import java.util.List;
import java.util.Optional;

import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckpointAppender;
import org.neo4j.kernel.impl.transaction.log.files.LogTailInformation;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFiles;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesContext;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * Only writes detached checkpoints.
 * But can find checkpoints in both the separate checkpoint files and the transaction logs to handle
 * logs that might have checkpoints in both places.
 */
public class CompositeCheckpointLogFile extends LifecycleAdapter implements CheckpointFile
{
    private final CheckpointLogFile checkpointLogFile;
    private final LegacyCheckpointLogFile legacyCheckpointLogFile;

    public CompositeCheckpointLogFile( TransactionLogFiles logFiles, TransactionLogFilesContext context )
    {
        this.checkpointLogFile = new CheckpointLogFile( logFiles, context );
        this.legacyCheckpointLogFile = new LegacyCheckpointLogFile( logFiles, context );
    }

    @Override
    public void start() throws Exception
    {
        checkpointLogFile.start();
    }

    @Override
    public void shutdown() throws Exception
    {
        checkpointLogFile.shutdown();
    }

    @Override
    public Optional<CheckpointInfo> findLatestCheckpoint() throws IOException
    {
        Optional<CheckpointInfo> latestCheckpoint = checkpointLogFile.findLatestCheckpoint();
        if ( latestCheckpoint.isEmpty() )
        {
            latestCheckpoint = legacyCheckpointLogFile.findLatestCheckpoint();
        }
        return latestCheckpoint;
    }

    /**
     * Note that checkpoints returned from this method can point to both separate checkpoint files
     * and the transaction logs. There is no way to know from the CheckpointInfo if the
     * checkpointEntryPosition logVersion refers to a log file or a checkpoint file.
     * This is so far okay since we usually only use the transactionLogPosition of the CheckpointInfo or
     * use this method when removing checkpoints in the tests where we know we only have checkpoints in separate files.
     */
    @Override
    public List<CheckpointInfo> reachableCheckpoints() throws IOException
    {
        List<CheckpointInfo> checkpointInfos = checkpointLogFile.reachableCheckpoints();
        List<CheckpointInfo> legacyCheckpointInfos = legacyCheckpointLogFile.reachableCheckpoints();

        // Any legacy checkpoints will be older than the detached ones, so adding the detached checkpoints last to preserve the order.
        legacyCheckpointInfos.addAll( checkpointInfos );
        return legacyCheckpointInfos;
    }

    @Override
    public List<CheckpointInfo> getReachableDetachedCheckpoints() throws IOException
    {
        return checkpointLogFile.getReachableDetachedCheckpoints();
    }

    @Override
    public CheckpointAppender getCheckpointAppender()
    {
        return checkpointLogFile.getCheckpointAppender();
    }

    @Override
    public LogTailInformation getTailInformation()
    {
        LogTailInformation tailInformation = checkpointLogFile.getTailInformation();
        if ( tailInformation.lastCheckPoint == null )
        {
            tailInformation = legacyCheckpointLogFile.getTailInformation();
        }
        return tailInformation;
    }

    @Override
    public Path getCurrentFile()
    {
        return checkpointLogFile.getCurrentFile();
    }

    @Override
    public Path getDetachedCheckpointFileForVersion( long logVersion )
    {
        return checkpointLogFile.getDetachedCheckpointFileForVersion( logVersion );
    }

    @Override
    public Path[] getDetachedCheckpointFiles()
    {
        return checkpointLogFile.getDetachedCheckpointFiles();
    }

    @Override
    public long getCurrentDetachedLogVersion()
    {
        return checkpointLogFile.getCurrentDetachedLogVersion();
    }

    @Override
    public long getDetachedCheckpointLogFileVersion( Path checkpointLogFile )
    {
        return this.checkpointLogFile.getDetachedCheckpointLogFileVersion( checkpointLogFile );
    }

    @Override
    public boolean rotationNeeded()
    {
        return checkpointLogFile.rotationNeeded();
    }

    @Override
    public synchronized Path rotate() throws IOException
    {
        return checkpointLogFile.rotate();
    }
}
