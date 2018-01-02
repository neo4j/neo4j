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

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.LogFileInformation;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;

import static org.neo4j.kernel.impl.transaction.log.LogVersionRepository.INITIAL_LOG_VERSION;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.LOG_HEADER_SIZE;

public class ThresholdBasedPruneStrategy implements LogPruneStrategy
{
    private final FileSystemAbstraction fileSystem;
    private final LogFileInformation logFileInformation;
    private final PhysicalLogFiles files;
    private final Threshold threshold;

    public ThresholdBasedPruneStrategy( FileSystemAbstraction fileSystem, LogFileInformation logFileInformation,
                                        PhysicalLogFiles files, Threshold threshold )
    {
        this.fileSystem = fileSystem;
        this.logFileInformation = logFileInformation;
        this.files = files;
        this.threshold = threshold;
    }

    @Override
    public void prune( long upToLogVersion )
    {
        if ( upToLogVersion == INITIAL_LOG_VERSION )
        {
            return;
        }

        threshold.init();
        long upper = upToLogVersion - 1;
        boolean exceeded = false;
        while ( upper >= 0 )
        {
            File file = files.getLogFileForVersion( upper );
            if ( !fileSystem.fileExists( file ) )
            {
                // There aren't logs to prune anything. Just return
                return;
            }

            if ( fileSystem.getFileSize( file ) > LOG_HEADER_SIZE &&
                    threshold.reached( file, upper, logFileInformation ) )
            {
                exceeded = true;
                break;
            }
            upper--;
        }

        if ( !exceeded )
        {
            return;
        }

        // Find out which log is the earliest existing (lower bound to prune)
        long lower = upper;
        while ( fileSystem.fileExists( files.getLogFileForVersion( lower - 1 ) ) )
        {
            lower--;
        }

        /*
         * Here we make sure that at least one historical log remains behind, in addition of course to the
         * current one. This is in order to make sure that at least one transaction remains always available for
         * serving to whomever asks for it.
         * To illustrate, imagine that there is a threshold in place configured so that it enforces prunning of the
         * log file that was just rotated out (for example, a file size threshold that is misconfigured to be smaller
         * than the smallest log). In such a case, until the database commits a transaction there will be no
         * transactions present on disk, making impossible to serve any to whichever client might ask, leading to
         * errors on both sides of the conversation.
         * This if statement does nothing more complicated than checking if the next-to-last log would be prunned
         * and simply skipping it if so.
         */
        if ( upper == upToLogVersion - 1)
        {
            upper--;
        }

        // The reason we delete from lower to upper is that if it crashes in the middle
        // we can be sure that no holes are created
        for ( long version = lower; version <= upper; version++ )
        {
            fileSystem.deleteFile( files.getLogFileForVersion( version ) );
        }
    }
}
