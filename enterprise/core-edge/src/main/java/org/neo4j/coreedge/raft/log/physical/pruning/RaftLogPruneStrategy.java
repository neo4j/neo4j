/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.coreedge.raft.log.physical.pruning;

import static org.neo4j.kernel.impl.transaction.log.LogVersionRepository.INITIAL_LOG_VERSION;

import org.neo4j.coreedge.raft.log.physical.PhysicalRaftLogFiles;
import org.neo4j.kernel.impl.transaction.log.LogFileInformation;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruneStrategy;
import org.neo4j.kernel.impl.transaction.log.pruning.Threshold;

public class RaftLogPruneStrategy implements LogPruneStrategy
{
    private final LogFileInformation logFileInformation;
    private final PhysicalRaftLogFiles files;
    private final Threshold threshold;

    public RaftLogPruneStrategy( LogFileInformation logFileInformation, PhysicalRaftLogFiles files,
                                 Threshold threshold )
    {
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
            if ( !files.versionExists( upper ) )
            {
                // There aren't logs to prune anything. Just return
                return;
            }

            if ( files.containsEntries( upper ) &&
                    threshold.reached( files.getLogFileForVersion( upper ), upper, logFileInformation ) )
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

        files.pruneUpTo( upper );
    }
}
