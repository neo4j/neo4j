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
package org.neo4j.kernel.api.index;

import org.neo4j.internal.schema.SchemaDescriptorSupplier;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.TokenIndexEntryUpdate;
import org.neo4j.storageengine.api.ValueIndexEntryUpdate;

/**
 * IndexUpdaters are responsible for updating indexes during the commit process. There is one new instance handling
 * each commit, created from {@link org.neo4j.kernel.api.index.IndexAccessor}.
 *
 * {@link #process(IndexEntryUpdate)} is called for each entry, wherein the actual updates are applied.
 *
 * Each IndexUpdater is not thread-safe, and is assumed to be instantiated per transaction.
 */
public interface IndexUpdater extends AutoCloseable {
    void process(IndexEntryUpdate<?> update) throws IndexEntryConflictException;

    default void yield() {}

    default <INDEX_KEY extends SchemaDescriptorSupplier> ValueIndexEntryUpdate<INDEX_KEY> asValueUpdate(
            IndexEntryUpdate<INDEX_KEY> update) {
        if (update instanceof ValueIndexEntryUpdate) {
            return (ValueIndexEntryUpdate<INDEX_KEY>) update;
        }
        throw new UnsupportedOperationException(
                "Tried to process " + update + " with " + getClass().getSimpleName() + ", but this is not supported.");
    }

    default <INDEX_KEY extends SchemaDescriptorSupplier> TokenIndexEntryUpdate<INDEX_KEY> asTokenUpdate(
            IndexEntryUpdate<INDEX_KEY> update) {
        if (update instanceof TokenIndexEntryUpdate) {
            return (TokenIndexEntryUpdate<INDEX_KEY>) update;
        }
        throw new UnsupportedOperationException(
                "Tried to process " + update + " with " + getClass().getSimpleName() + ", but this is not supported.");
    }

    @Override
    void close() throws IndexEntryConflictException;
}
