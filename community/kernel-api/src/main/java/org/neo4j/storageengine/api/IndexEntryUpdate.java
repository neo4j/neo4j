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

import static org.neo4j.internal.schema.SchemaUserDescription.TOKEN_ID_NAME_LOOKUP;

import java.util.Objects;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.schema.IndexDescriptor;

/**
 * Subclasses of this represent events related to property changes due to property or label addition, deletion or
 * update.
 * This is of use in populating indexes that might be relevant to node label and property combinations.
 *
 */
public abstract sealed class IndexEntryUpdate
        permits TokenIndexEntryUpdate, EagerValueIndexEntryUpdate, LazyValueIndexEntryUpdate {
    private final long entityId;
    private final UpdateMode updateMode;
    private final IndexDescriptor indexKey;

    IndexEntryUpdate(long entityId, IndexDescriptor indexKey, UpdateMode updateMode) {
        this.entityId = entityId;
        this.indexKey = indexKey;
        this.updateMode = updateMode;
    }

    public final long getEntityId() {
        return entityId;
    }

    public UpdateMode updateMode() {
        return updateMode;
    }

    public IndexDescriptor indexKey() {
        return indexKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IndexEntryUpdate that = (IndexEntryUpdate) o;

        if (entityId != that.entityId) {
            return false;
        }
        if (updateMode != that.updateMode) {
            return false;
        }

        boolean schemaEquals = Objects.equals(indexKey, that.indexKey);
        if (!schemaEquals) {
            return false;
        }

        return valueEquals(that);
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(entityId);
        result = 31 * result + (updateMode != null ? updateMode.hashCode() : 0);
        result = 31 * result + (indexKey != null ? indexKey.hashCode() : 0);
        result = 31 * result + valueHash();
        return result;
    }

    public String describe(TokenNameLookup tokenNameLookup) {
        return String.format(
                getClass().getSimpleName() + "[id=%d, mode=%s, %s, %s]",
                entityId,
                updateMode,
                indexKey().schema().userDescription(tokenNameLookup),
                valueToString());
    }

    /**
     * Returns rough estimate of memory usage of this instance in bytes.
     */
    public abstract long roughSizeOfUpdate();

    /**
     * Need to align with {@link #valueHash() value hash code}.
     */
    protected abstract boolean valueEquals(IndexEntryUpdate that);

    /**
     * Need to align with {@link #valueEquals(IndexEntryUpdate) value equals}.
     */
    protected abstract int valueHash();

    /**
     * Return string representation of value state.
     */
    protected abstract String valueToString();

    /**
     * @param entityId new entity ID.
     * @return an instance just like this one, but with a different entity ID.
     */
    public abstract IndexEntryUpdate withEntityId(long entityId);

    /**
     * Convert to an eagerly loaded index entry update.
     */
    public abstract IndexEntryUpdate eagerly();

    @Override
    public String toString() {
        return describe(TOKEN_ID_NAME_LOOKUP);
    }
}
