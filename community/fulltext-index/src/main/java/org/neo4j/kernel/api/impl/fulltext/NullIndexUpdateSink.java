/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.impl.fulltext;

import java.util.Collection;
import org.neo4j.kernel.api.impl.index.DatabaseIndex;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.storageengine.api.IndexEntryUpdate;

/**
 * An implementation of {@link IndexUpdateSink} that does not actually do anything.
 */
public class NullIndexUpdateSink extends IndexUpdateSink {
    public static final NullIndexUpdateSink INSTANCE = new NullIndexUpdateSink();

    private NullIndexUpdateSink() {
        super(null, 0);
    }

    @Override
    public void enqueueTransactionBatchOfUpdates(
            DatabaseIndex<? extends IndexReader> index,
            IndexUpdater indexUpdater,
            Collection<IndexEntryUpdate<?>> updates) {}

    @Override
    public void awaitUpdateApplication() {}
}
