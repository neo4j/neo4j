/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.api.index;

import static org.neo4j.helpers.collection.IteratorUtil.count;

import java.io.IOException;

import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.NodePropertyUpdate;

public class CountingIndexUpdater implements IndexUpdater
{
    private long delta;
    private final IndexUpdater delegate;
    private final IndexUpdateCountVisitor indexUpdateCountVisitor;

    public CountingIndexUpdater( IndexUpdater delegate, IndexUpdateCountVisitor indexUpdateCountVisitor )
    {
        this.delegate = delegate;
        this.indexUpdateCountVisitor = indexUpdateCountVisitor;
    }

    @Override
    public void process( NodePropertyUpdate update ) throws IOException, IndexEntryConflictException
    {
        switch ( update.getUpdateMode() )
        {
            case ADDED:
                delta += 1;
                break;

            case REMOVED:
                delta -= 1;
                break;

            default:
                break;
        }
        delegate.process( update );
    }

    @Override
    public void close() throws IOException, IndexEntryConflictException
    {
        indexUpdateCountVisitor.visitIndexUpdateCount( delta );
        delegate.close();
    }

    @Override
    public void remove( Iterable<Long> nodeIds ) throws IOException
    {
        delta -= count( nodeIds.iterator() );
        delegate.remove( nodeIds );
    }

    interface IndexUpdateCountVisitor
    {
        void visitIndexUpdateCount( long indexUpdates );
    }
}
