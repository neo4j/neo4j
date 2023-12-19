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

    @Override
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
