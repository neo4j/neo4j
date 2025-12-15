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

import static java.util.Objects.requireNonNull;

import java.util.Arrays;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.memory.HeapEstimator;

public final class TokenIndexEntryUpdate extends IndexEntryUpdate {
    public static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(TokenIndexEntryUpdate.class);

    private final int[] removed;
    private final int[] added;

    TokenIndexEntryUpdate(long entityId, IndexDescriptor indexKey, int[] removed, int[] added) {
        super(entityId, indexKey, UpdateMode.CHANGED);
        this.removed = requireNonNull(removed);
        this.added = requireNonNull(added);
    }

    public int[] added() {
        return added;
    }

    public int[] removed() {
        return removed;
    }

    @Override
    public long roughSizeOfUpdate() {
        return HeapEstimator.sizeOf(added) + HeapEstimator.sizeOf(removed);
    }

    @Override
    protected boolean valueEquals(IndexEntryUpdate o) {
        if (!(o instanceof TokenIndexEntryUpdate that)) {
            return false;
        }
        if (!Arrays.equals(removed, that.removed)) {
            return false;
        }
        return Arrays.equals(added, that.added);
    }

    @Override
    protected int valueHash() {
        int result = Arrays.hashCode(removed);
        result = 31 * result + Arrays.hashCode(added);
        return result;
    }

    @Override
    protected String valueToString() {
        return String.format("removed=%s, added=%s", Arrays.toString(removed), Arrays.toString(added));
    }

    @Override
    public IndexEntryUpdate withEntityId(long entityId) {
        return tokenChange(entityId, indexKey(), removed, added);
    }

    @Override
    public IndexEntryUpdate eagerly() {
        return this;
    }

    public static TokenIndexEntryUpdate tokenChange(
            long entityId, IndexDescriptor indexKey, int[] removed, int[] added) {
        return new TokenIndexEntryUpdate(entityId, indexKey, removed, added);
    }
}
