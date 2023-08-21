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
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.internal.schema.SchemaDescriptorSupplier;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.UpdateMode;
import org.neo4j.storageengine.api.ValueIndexEntryUpdate;
import org.neo4j.values.storable.Value;

@FunctionalInterface
public interface IndexUpdateIgnoreStrategy {
    /**
     * @param values to process
     * @return true if values should be ignored by the updater
     */
    boolean ignore(Value[] values);

    /**
     * Default: check the before and after values for a change; otherwise the values
     * @param update the update to process
     * @return true if update should be ignored by updater
     */
    default <INDEX_KEY extends SchemaDescriptorSupplier> boolean ignore(ValueIndexEntryUpdate<INDEX_KEY> update) {
        if (update.updateMode() == UpdateMode.CHANGED) {
            return ignore(update.beforeValues()) && ignore(update.values());
        }
        return ignore(update.values());
    }

    /**
     * Some {@link ValueIndexEntryUpdate}s may be better represented by another in some indexes; especially those that do not support all value types.
     * Default: {@link UpdateMode#CHANGED} updates, for Indexes that do not support all values; are better represented as an {@link UpdateMode#REMOVED} or
     * {@link UpdateMode#ADDED} update.
     *
     * @param update a {@link ValueIndexEntryUpdate} to convert
     * @return an equivalent {@link ValueIndexEntryUpdate}
     */
    default <INDEX_KEY extends SchemaDescriptorSupplier> ValueIndexEntryUpdate<INDEX_KEY> toEquivalentUpdate(
            ValueIndexEntryUpdate<INDEX_KEY> update) {
        // Only CHANGED may need replacing
        if (update.updateMode() != UpdateMode.CHANGED) {
            return update;
        }

        final var beforeValues = update.beforeValues();
        final var afterValues = update.values();

        final var shouldRemove = !ignore(beforeValues);
        final var shouldAdd = !ignore(afterValues);

        if (shouldRemove && shouldAdd) {
            return update;
        }

        final var key = update.indexKey();
        final var entityId = update.getEntityId();

        if (shouldRemove) {
            return IndexEntryUpdate.remove(entityId, key, beforeValues);
        }

        if (shouldAdd) {
            return IndexEntryUpdate.add(entityId, key, afterValues);
        }

        throw new IllegalStateException(
                "Attempted a " + UpdateMode.CHANGED + " update, which was not applicable to the index");
    }

    /**
     * Ignores nothing
     */
    IndexUpdateIgnoreStrategy NO_IGNORE = new IndexUpdateIgnoreStrategy() {
        @Override
        public boolean ignore(Value[] values) {
            return false;
        }

        @Override
        public <INDEX_KEY extends SchemaDescriptorSupplier> boolean ignore(ValueIndexEntryUpdate<INDEX_KEY> update) {
            return false;
        }

        @Override
        public <INDEX_KEY extends SchemaDescriptorSupplier> ValueIndexEntryUpdate<INDEX_KEY> toEquivalentUpdate(
                ValueIndexEntryUpdate<INDEX_KEY> update) {
            return update;
        }
    };
}
