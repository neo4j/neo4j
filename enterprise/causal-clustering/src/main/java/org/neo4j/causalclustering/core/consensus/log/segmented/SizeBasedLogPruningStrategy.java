/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
