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
package org.neo4j.internal.counts;

import java.util.concurrent.locks.Lock;

import org.neo4j.counts.CountsAccessor;

import static org.neo4j.internal.counts.CountsKey.nodeKey;
import static org.neo4j.internal.counts.CountsKey.relationshipKey;

/**
 * The implementation of {@link CountsAccessor.Updater} for the {@link GBPTreeCountsStore}.
 * Writing happens inside the supplied {@link CountWriter}.
 */
class CountUpdater implements CountsAccessor.Updater
{
    private final CountWriter writer;
    private final Lock lock;

    CountUpdater( CountWriter writer, Lock lock )
    {
        this.writer = writer;
        this.lock = lock;
    }

    @Override
    public void incrementNodeCount( long labelId, long delta )
    {
        writer.write( nodeKey( labelId ), delta );
    }

    @Override
    public void incrementRelationshipCount( long startLabelId, int typeId, long endLabelId, long delta )
    {
        writer.write( relationshipKey( startLabelId, typeId, endLabelId ), delta );
    }

    @Override
    public void close()
    {
        try
        {
            writer.close();
        }
        finally
        {
            lock.unlock();
        }
    }

    public interface CountWriter extends AutoCloseable
    {
        void write( CountsKey key, long delta );

        @Override
        void close();
    }
}
