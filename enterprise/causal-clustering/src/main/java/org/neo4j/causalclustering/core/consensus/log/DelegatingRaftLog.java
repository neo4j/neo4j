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
package org.neo4j.causalclustering.core.consensus.log;

import java.io.IOException;

public class DelegatingRaftLog implements RaftLog
{
    private final RaftLog inner;

    public DelegatingRaftLog( RaftLog inner )
    {
        this.inner = inner;
    }

    @Override
    public long append( RaftLogEntry... entry ) throws IOException
    {
        return inner.append( entry );
    }

    @Override
    public void truncate( long fromIndex ) throws IOException
    {
        inner.truncate( fromIndex );
    }

    @Override
    public long prune( long safeIndex ) throws IOException
    {
        return inner.prune( safeIndex );
    }

    @Override
    public long skip( long index, long term ) throws IOException
    {
        return inner.skip( index, term );
    }

    @Override
    public long appendIndex()
    {
        return inner.appendIndex();
    }

    @Override
    public long prevIndex()
    {
        return inner.prevIndex();
    }

    @Override
    public long readEntryTerm( long logIndex ) throws IOException
    {
        return inner.readEntryTerm( logIndex );
    }

    @Override
    public RaftLogCursor getEntryCursor( long fromIndex ) throws IOException
    {
        return inner.getEntryCursor( fromIndex );
    }
}
