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
import org.neo4j.kernel.impl.transaction.log.files.RotatableFile;
import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * Access to underlying store checkpoints, that can be stored in multiple log files, separate log files etc.
 */
public interface CheckpointFile extends Lifecycle, RotatableFile
{
    /**
     * Last available checkpoint
     * @return last checkpoint
     */
    Optional<CheckpointInfo> findLatestCheckpoint() throws IOException;

    /**
     * List of all reachable checkpoints from earliest to latest available
     * @return list of checkpoints, empty list if not reachable checkpoints are available
     */
    List<CheckpointInfo> reachableCheckpoints() throws IOException;

    /**
     * List of all reachable checkpoints in separate checkpoint files from earliest to latest available
     * @return list of checkpoints, empty list if not reachable checkpoints are available in the separate files
     */
    List<CheckpointInfo> getReachableDetachedCheckpoints() throws IOException;

    /**
     * @return appender that aware how and where to append checkpoint record in particular implementation of the checkpoint file
     */
    CheckpointAppender getCheckpointAppender();

    /**
     * @return Information about log tail: records after checkpoint, missing logs etc
     */
    LogTailInformation getTailInformation();

    /**
     * @return checkpoint file that is currently used to store checkpoints into
     */
    Path getCurrentFile();

    /**
     * @param logVersion version of the checkpoint file to get
     * @return checkpoint file of the requested version
     */
    Path getDetachedCheckpointFileForVersion( long logVersion );

    /**
     * @return set of files that are used to store checkpoints.
     * Can be empty if there is no specific files for checkpoints and they are stored somewhere else
     */
    Path[] getDetachedCheckpointFiles();

    /**
     * @return checkpoint file version that is currently used to store checkpoints into
     */
    long getCurrentDetachedLogVersion();

    /**
     * @param checkpointLogFile checkpoint log file
     * @return Version of the provided checkpoint file
     */
    long getDetachedCheckpointLogFileVersion( Path checkpointLogFile );
}
