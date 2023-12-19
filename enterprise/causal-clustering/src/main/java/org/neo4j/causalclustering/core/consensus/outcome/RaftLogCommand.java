/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.core.consensus.outcome;

import java.io.IOException;

import org.neo4j.causalclustering.core.consensus.log.cache.InFlightCache;
import org.neo4j.causalclustering.core.consensus.log.RaftLog;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.logging.Log;

public interface RaftLogCommand
{
    interface Handler
    {
        void append( long baseIndex, RaftLogEntry... entries ) throws IOException;
        void truncate( long fromIndex ) throws IOException;
        void prune( long pruneIndex );
    }

    void dispatch( Handler handler ) throws IOException;

    void applyTo( RaftLog raftLog, Log log ) throws IOException;

    void applyTo( InFlightCache inFlightCache, Log log );
}
