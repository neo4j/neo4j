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
import java.util.stream.LongStream;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFileInformation;

import static org.neo4j.kernel.impl.transaction.log.LogVersionRepository.INITIAL_LOG_VERSION;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.LOG_HEADER_SIZE;

public class ThresholdBasedPruneStrategy implements LogPruneStrategy
{
    private final FileSystemAbstraction fileSystem;
    private final LogFiles files;
    private final Threshold threshold;
    private final TransactionLogFileInformation logFileInformation;

    ThresholdBasedPruneStrategy( FileSystemAbstraction fileSystem, LogFiles logFiles, Threshold threshold )
    {
        this.fileSystem = fileSystem;
        this.files = logFiles;
        this.logFileInformation = files.getLogFileInformation();
        this.threshold = threshold;
    }

    @Override
    public synchronized LongStream findLogVersionsToDelete( long upToVersion )
    {
        if ( upToVersion == INITIAL_LOG_VERSION )
        {
            return LongStream.empty();
        }

        threshold.init();
        long upper = upToVersion - 1;
        boolean exceeded = false;
        while ( upper >= 0 )
        {
            File file = files.getLogFileForVersion( upper );
            if ( !fileSystem.fileExists( file ) )
            {
                // There aren't logs to prune anything. Just return
                return LongStream.empty();
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
            return LongStream.empty();
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
         * To illustrate, imagine that there is a threshold in place configured so that it enforces pruning of the
         * log file that was just rotated out (for example, a file size threshold that is misconfigured to be smaller
         * than the smallest log). In such a case, until the database commits a transaction there will be no
         * transactions present on disk, making impossible to serve any to whichever client might ask, leading to
         * errors on both sides of the conversation.
         * This if statement does nothing more complicated than checking if the next-to-last log would be pruned
         * and simply skipping it if so.
         */
        if ( upper == upToVersion - 1 )
        {
            upper--;
        }

        // The reason we delete from lower to upper is that if it crashes in the middle we can be sure that no holes
        // are created.
        // We create a closed range because we want to include the 'upper' log version as well. The check above ensures
        // we don't accidentally leave the database without any logs.
        return LongStream.rangeClosed( lower, upper );
    }
}
