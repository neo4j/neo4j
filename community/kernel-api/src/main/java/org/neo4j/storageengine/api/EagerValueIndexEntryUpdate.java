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

import static java.lang.String.format;

import java.util.Arrays;
import java.util.Objects;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptorSupplier;
import org.neo4j.values.storable.Value;

public final class EagerValueIndexEntryUpdate extends IndexEntryUpdate implements ValueIndexEntryUpdate {
    private final Value[] before;
    private final Value[] values;

    private EagerValueIndexEntryUpdate(long entityId, IndexDescriptor indexKey, UpdateMode updateMode, Value[] values) {
        this(entityId, indexKey, updateMode, null, values);
    }

    EagerValueIndexEntryUpdate(
            long entityId, IndexDescriptor indexKey, UpdateMode updateMode, Value[] before, Value[] values) {
        super(entityId, indexKey, updateMode);
        validateValuesLength(indexKey, before, values);

        this.before = before;
        this.values = values;
    }

    public static EagerValueIndexEntryUpdate add(long entityId, IndexDescriptor indexKey, Value... values) {
        return new EagerValueIndexEntryUpdate(entityId, indexKey, UpdateMode.ADDED, values);
    }

    public static EagerValueIndexEntryUpdate remove(long entityId, IndexDescriptor indexKey, Value... values) {
        return new EagerValueIndexEntryUpdate(entityId, indexKey, UpdateMode.REMOVED, values);
    }

    public static EagerValueIndexEntryUpdate change(
            long entityId, IndexDescriptor indexKey, Value before, Value after) {
        return new EagerValueIndexEntryUpdate(
                entityId, indexKey, UpdateMode.CHANGED, new Value[] {before}, new Value[] {after});
    }

    public static EagerValueIndexEntryUpdate change(
            long entityId, IndexDescriptor indexKey, Value[] before, Value[] after) {
        return new EagerValueIndexEntryUpdate(entityId, indexKey, UpdateMode.CHANGED, before, after);
    }

    @Override
    public Value[] values() {
        return values;
    }

    @Override
    public Value[] beforeValues() {
        if (before == null) {
            throw new UnsupportedOperationException("beforeValues is only valid for `UpdateMode.CHANGED");
        }
        return before;
    }

    @Override
    public long roughSizeOfUpdate() {
        return heapSizeOf(values) + (updateMode() == UpdateMode.CHANGED ? heapSizeOf(before) : 0);
    }

    @Override
    protected boolean valueEquals(IndexEntryUpdate o) {
        if (!(o instanceof EagerValueIndexEntryUpdate that)) {
            return false;
        }
        return Arrays.equals(before, that.before) && Arrays.equals(values, that.values);
    }

    @Override
    protected int valueHash() {
        return Objects.hash(Arrays.hashCode(before), Arrays.hashCode(values));
    }

    @Override
    protected String valueToString() {
        return String.format("beforeValues=%s, values=%s", Arrays.toString(before), Arrays.toString(values));
    }

    @Override
    public String toString() {
        return "ValueIndexEntryUpdate{" + "entity=" + getEntityId() + ", before=" + Arrays.toString(before)
                + ", values=" + Arrays.toString(values) + ", updateMode=" + updateMode() + ", index:" + indexKey()
                + '}';
    }

    @Override
    public IndexEntryUpdate withEntityId(long entityId) {
        return switch (updateMode()) {
            case ADDED -> add(entityId, indexKey(), values);
            case CHANGED -> change(entityId, indexKey(), before, values);
            case REMOVED -> remove(entityId, indexKey(), values);
        };
    }

    @Override
    public IndexEntryUpdate eagerly() {
        return this;
    }

    static <P> void validateValuesLength(SchemaDescriptorSupplier indexKey, P[] before, P[] values) {
        // we do not support partial index entries
        assert indexKey.schema().getPropertyIds().length == values.length
                : format(
                        "ValueIndexEntryUpdate values must be of same length as index compositeness. "
                                + "Index on %s, but got values %s",
                        indexKey.schema().toString(), Arrays.toString(values));
        assert before == null || before.length == values.length;
    }

    static long heapSizeOf(Value[] values) {
        long size = 0;
        if (values != null) {
            for (Value value : values) {
                if (value != null) {
                    size += value.estimatedHeapUsage();
                }
            }
        }
        return size;
    }
}
