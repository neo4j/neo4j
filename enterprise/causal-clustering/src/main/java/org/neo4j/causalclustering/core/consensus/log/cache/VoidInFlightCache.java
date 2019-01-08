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
package org.neo4j.causalclustering.core.consensus.log.cache;

import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;

/**
 * A cache which caches nothing. This means that all lookups
 * will go to the on-disk Raft log, which might be quite good
 * anyway since recently written items will be in OS page cache
 * memory generally. But it will incur an unmarshalling overhead.
 */
public class VoidInFlightCache implements InFlightCache
{
    @Override
    public void enable()
    {
    }

    @Override
    public void put( long logIndex, RaftLogEntry entry )
    {
    }

    @Override
    public RaftLogEntry get( long logIndex )
    {
        return null;
    }

    @Override
    public void truncate( long fromIndex )
    {
    }

    @Override
    public void prune( long upToIndex )
    {
    }

    @Override
    public long totalBytes()
    {
        return 0;
    }

    @Override
    public int elementCount()
    {
        return 0;
    }
}
