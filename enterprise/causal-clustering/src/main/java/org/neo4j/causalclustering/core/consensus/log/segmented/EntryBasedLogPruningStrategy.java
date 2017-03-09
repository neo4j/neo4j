/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.core.consensus.log.segmented;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

class EntryBasedLogPruningStrategy implements CoreLogPruningStrategy
{
    private final long entriesToKeep;
    private final Log log;

    EntryBasedLogPruningStrategy( long entriesToKeep, LogProvider logProvider )
    {
        this.entriesToKeep = entriesToKeep;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public long getIndexToKeep( Segments segments )
    {
        SegmentVisitor visitor = new SegmentVisitor();
        segments.visitBackwards( visitor );

        if ( visitor.visitedCount == 0 )
        {
            log.warn( "No log files found during the prune operation. This state should resolve on its own, but" +
                      " if this warning continues, you may want to look for other errors in the user log." );
        }

        return visitor.prevIndex;
    }

    private class SegmentVisitor implements Visitor<SegmentFile,RuntimeException>
    {
        long visitedCount;
        long accumulated;
        long prevIndex = -1;
        long lastPrevIndex = -1;

        @Override
        public boolean visit( SegmentFile segment ) throws RuntimeException
        {
            visitedCount++;

            if ( lastPrevIndex == -1 )
            {
                lastPrevIndex = segment.header().prevIndex();
                return false; // first entry, continue visiting next
            }

            prevIndex = segment.header().prevIndex();
            accumulated += lastPrevIndex - prevIndex;
            lastPrevIndex = prevIndex;

            return accumulated >= entriesToKeep;
        }
    }
}
