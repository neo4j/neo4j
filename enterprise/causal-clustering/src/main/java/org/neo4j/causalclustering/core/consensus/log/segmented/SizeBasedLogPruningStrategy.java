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

class SizeBasedLogPruningStrategy implements CoreLogPruningStrategy, Visitor<SegmentFile,RuntimeException>
{
    private final long bytesToKeep;
    private long accumulatedSize;
    private SegmentFile file;

    SizeBasedLogPruningStrategy( long bytesToKeep )
    {
        this.bytesToKeep = bytesToKeep;
    }

    public synchronized long getIndexToKeep( Segments segments )
    {
        accumulatedSize = 0;
        file = null;

        segments.visitBackwards( this );

        return file != null ? (file.header().prevIndex() + 1) : -1;
    }

    @Override
    public boolean visit( SegmentFile segment ) throws RuntimeException
    {
        if ( accumulatedSize < bytesToKeep )
        {
            file = segment;
            accumulatedSize += file.size();
            return false;
        }

        return true;
    }
}
