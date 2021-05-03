/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.counts;

import java.io.IOException;

import org.neo4j.annotations.documented.ReporterFactory;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.memory.MemoryTracker;

/**
 * Store and accessor of entity counts. Counts changes revolves around one or a combination of multiple tokens and are applied as deltas.
 * This makes it necessary to tie all changes to transaction ids so that this store can tell whether or not to re-apply any given
 * set of changes during recovery. Changes are applied by making calls to {@link CountsAccessor.Updater} from {@link #apply(long, CursorContext)}.
 */
public interface CountsStore extends CountsStorage, CountsAccessor
{
    CountsStore NULL_INSTANCE = new NullCountsStore();

    /**
     * @param txId id of the transaction that produces the changes that are being applied.
     * @param cursorContext underlying page cursor context
     * @return an updater where count deltas are being applied onto.
     */
    CountsAccessor.Updater apply( long txId, CursorContext cursorContext );

    class NullCountsStore implements CountsStore
    {
        @Override
        public Updater apply( long txId, CursorContext cursorContext )
        {
            return CountsAccessor.NO_OP_UPDATER;
        }

        @Override
        public void close()
        {   // no-op
        }

        @Override
        public void start( CursorContext cursorContext, MemoryTracker memoryTracker ) throws IOException
        {   // no-op
        }

        @Override
        public void checkpoint( CursorContext cursorContext ) throws IOException
        {   // no-op
        }

        @Override
        public long nodeCount( int labelId, CursorContext cursorContext )
        {
            return 0;
        }

        @Override
        public long relationshipCount( int startLabelId, int typeId, int endLabelId, CursorContext cursorContext )
        {
            return 0;
        }

        @Override
        public void accept( CountsVisitor visitor, CursorContext cursorContext )
        {   // no-op
        }

        @Override
        public boolean consistencyCheck( ReporterFactory reporterFactory, CursorContext cursorContext )
        {
            return true;
        }
    }
}
