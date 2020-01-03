/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.api.impl.fulltext;

import org.neo4j.kernel.api.impl.index.DatabaseIndex;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.storageengine.api.schema.IndexReader;

class EventuallyConsistentIndexUpdater implements IndexUpdater
{
    private final DatabaseIndex<? extends IndexReader> index;
    private final IndexUpdater indexUpdater;
    private final IndexUpdateSink indexUpdateSink;

    EventuallyConsistentIndexUpdater( DatabaseIndex<? extends IndexReader> index, IndexUpdater indexUpdater, IndexUpdateSink indexUpdateSink )
    {
        this.index = index;
        this.indexUpdater = indexUpdater;
        this.indexUpdateSink = indexUpdateSink;
    }

    @Override
    public void process( IndexEntryUpdate<?> update )
    {
        indexUpdateSink.enqueueUpdate( index, indexUpdater, update );
    }

    @Override
    public void close()
    {
        indexUpdateSink.closeUpdater( index, indexUpdater );
    }
}
