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
package org.neo4j.kernel.impl.api.index.updater;

import java.io.IOException;

import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.index.IndexStoreView;

public class UpdateCountingIndexUpdater implements IndexUpdater
{
    private final IndexStoreView storeView;
    private final long indexId;
    private final IndexUpdater delegate;
    private long updates;

    public UpdateCountingIndexUpdater( IndexStoreView storeView, long indexId, IndexUpdater delegate )
    {
        this.storeView = storeView;
        this.indexId = indexId;
        this.delegate = delegate;
    }

    @Override
    public void process( IndexEntryUpdate<?> update ) throws IOException, IndexEntryConflictException
    {
        delegate.process( update );
        updates++;
    }

    @Override
    public void close() throws IOException, IndexEntryConflictException
    {
        delegate.close();
        storeView.incrementIndexUpdates( indexId, updates );
    }
}
