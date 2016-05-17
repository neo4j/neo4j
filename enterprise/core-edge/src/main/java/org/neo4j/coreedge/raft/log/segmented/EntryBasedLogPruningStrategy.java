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
package org.neo4j.coreedge.raft.log.segmented;

import java.util.ListIterator;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class EntryBasedLogPruningStrategy implements CoreLogPruningStrategy
{
    private final long entriesToKeep;
    private final Log log;

    public EntryBasedLogPruningStrategy( long entriesToKeep, LogProvider logProvider )
    {
        this.entriesToKeep = entriesToKeep;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public long getIndexToKeep( Segments segments )
    {
        ListIterator<SegmentFile> iterator = segments.getSegmentFileIteratorAtEnd();
        SegmentFile segmentFile = null;
        long nextPrevIndex = 0;
        long accumulated = 0;
        if ( !iterator.hasPrevious() )
        {
            log.warn( "No log files found during the prune operation. This state should resolve on its own, but" +
                    " if this warning continues, you may want to look for other errors in the user log." );
            return -1; // -1 is the lowest possible append index and so always safe to return.
        }
        segmentFile = iterator.previous();
        nextPrevIndex = segmentFile.header().prevIndex();
        long prevIndex;
        // Iterate backwards through the files, counting entries from the headers until the limit is reached.
        while ( accumulated < entriesToKeep && iterator.hasPrevious() )
        {
            segmentFile = iterator.previous();
            prevIndex = segmentFile.header().prevIndex();
            accumulated += (nextPrevIndex - prevIndex);
            nextPrevIndex = prevIndex;
        }
        return segmentFile.header().prevIndex();
    }
}
