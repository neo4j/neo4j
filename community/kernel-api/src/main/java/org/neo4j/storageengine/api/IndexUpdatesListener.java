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
package org.neo4j.storageengine.api;

import java.io.Closeable;
import java.io.IOException;
import org.neo4j.internal.schema.IndexDescriptor;

/**
 * Listener of {@link IndexEntryUpdate index updates}. When part of a transaction applier, upon closing
 * the applier there will eventually be a call to {@link #close()}, potentially preceded by a call to
 * {@link #applyAsync()}, in which case {@link #close()} must wait for the async updates to be applied.
 */
public interface IndexUpdatesListener extends Closeable {
    /**
     * Notification about an index update due to a data change.
     */
    void indexUpdate(IndexEntryUpdate<IndexDescriptor> update);

    /**
     * Notification about a list of index updates due to a data change.
     */
    void indexUpdates(Iterable<IndexEntryUpdate<IndexDescriptor>> updates);

    /**
     * Request to start applying these updates asynchronously. When later calling {@link #close()}
     * it'll need to wait for those to be fully applied.
     * @throws IOException on error applying the updates.
     */
    void applyAsync() throws IOException;
}
