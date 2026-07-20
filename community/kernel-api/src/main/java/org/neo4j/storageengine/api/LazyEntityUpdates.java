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

import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;
import static org.neo4j.storageengine.api.LazyValueIndexEntryUpdate.ValueSupplier.NULL_SUPPLIER;

import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.values.storable.Value;

/**
 * Version of {@link EntityUpdates} that allows lazy computation of values.
 * If given a {@link LazyValueIndexEntryUpdate.ValueSupplier} in the several builder methods,
 * the value will only be computed later.
 * If given a {@link Value} in the several builder methods, the value will be wrapped in a constant
 * {@link LazyValueIndexEntryUpdate.ValueSupplier}, thus computed eagerly.
 */
public final class LazyEntityUpdates extends AbstractEntityUpdates<LazyEntityUpdates.PropertyValueSupplier> {

    @SuppressWarnings("UnusedReturnValue")
    public static final class Builder
            extends AbstractEntityUpdates.Builder<LazyEntityUpdates.PropertyValueSupplier, LazyEntityUpdates, Builder> {

        private Builder(LazyEntityUpdates updates) {
            super(updates);
        }

        public Builder added(int propertyKeyId, LazyValueIndexEntryUpdate.ValueSupplier valueSupplier) {
            updates.put(propertyKeyId, LazyEntityUpdates.after(valueSupplier));
            return this;
        }

        public Builder removed(int propertyKeyId, LazyValueIndexEntryUpdate.ValueSupplier valueSupplier) {
            updates.put(propertyKeyId, LazyEntityUpdates.before(valueSupplier));
            return this;
        }

        public Builder changed(
                int propertyKeyId,
                LazyValueIndexEntryUpdate.ValueSupplier before,
                LazyValueIndexEntryUpdate.ValueSupplier after) {
            updates.put(propertyKeyId, LazyEntityUpdates.changed(before, after));
            return this;
        }

        public Builder existing(int propertyKeyId, LazyValueIndexEntryUpdate.ValueSupplier valueSupplier) {
            updates.put(propertyKeyId, LazyEntityUpdates.unchanged(valueSupplier));
            return this;
        }
    }

    public static Builder forEntity(long entityId, boolean propertyListIsComplete) {
        return new Builder(new LazyEntityUpdates(entityId, EMPTY_INT_ARRAY, EMPTY_INT_ARRAY, propertyListIsComplete));
    }

    private LazyEntityUpdates(
            long entityId, int[] entityTokensBefore, int[] entityTokensAfter, boolean propertyListComplete) {
        super(entityId, entityTokensBefore, entityTokensAfter, propertyListComplete);
    }

    private LazyValueIndexEntryUpdate.ValueSupplier[] valuesBefore(int[] propertyIds, boolean defaultToNoValue) {
        LazyValueIndexEntryUpdate.ValueSupplier[] supplier =
                new LazyValueIndexEntryUpdate.ValueSupplier[propertyIds.length];
        for (int i = 0; i < propertyIds.length; i++) {
            supplier[i] = knownProperty(propertyIds[i], defaultToNoValue).before;
        }
        return supplier;
    }

    private LazyValueIndexEntryUpdate.ValueSupplier[] valuesAfter(int[] propertyIds) {
        LazyValueIndexEntryUpdate.ValueSupplier[] supplier =
                new LazyValueIndexEntryUpdate.ValueSupplier[propertyIds.length];
        for (int i = 0; i < propertyIds.length; i++) {
            PropertyValueSupplier propertyValueSupplier = knownProperties.get(propertyIds[i]);
            supplier[i] = propertyValueSupplier == null ? NULL_SUPPLIER : propertyValueSupplier.after;
        }
        return supplier;
    }

    @Override
    protected IndexEntryUpdate remove(IndexDescriptor indexKey, int[] propertyIds, boolean defaultToNoValue) {
        return LazyValueIndexEntryUpdate.remove(entityId, indexKey, valuesBefore(propertyIds, defaultToNoValue));
    }

    @Override
    protected IndexEntryUpdate add(IndexDescriptor indexKey, int[] propertyIds) {
        return LazyValueIndexEntryUpdate.add(entityId, indexKey, valuesAfter(propertyIds));
    }

    @Override
    protected IndexEntryUpdate change(IndexDescriptor indexKey, int[] propertyIds, boolean defaultToNoValue) {
        return LazyValueIndexEntryUpdate.change(
                entityId, indexKey, valuesBefore(propertyIds, defaultToNoValue), valuesAfter(propertyIds));
    }

    public record PropertyValueSupplier(
            LazyValueIndexEntryUpdate.ValueSupplier before,
            LazyValueIndexEntryUpdate.ValueSupplier after,
            PropertyValueType type)
            implements PropertyValueInterface {

        @Override
        public boolean hasBefore() {
            return before != NULL_SUPPLIER;
        }

        @Override
        public boolean hasAfter() {
            return after != NULL_SUPPLIER;
        }
    }

    private static final PropertyValueSupplier NO_VALUE =
            new PropertyValueSupplier(NULL_SUPPLIER, NULL_SUPPLIER, PropertyValueType.NoValue);

    @Override
    protected PropertyValueSupplier noValue() {
        return NO_VALUE;
    }

    @Override
    protected PropertyValueSupplier before(Value value) {
        return before(LazyValueIndexEntryUpdate.ValueSupplier.constant(value));
    }

    private static PropertyValueSupplier before(LazyValueIndexEntryUpdate.ValueSupplier valueSupplier) {
        return new PropertyValueSupplier(valueSupplier, NULL_SUPPLIER, PropertyValueType.Before);
    }

    @Override
    protected PropertyValueSupplier after(Value value) {
        return after(LazyValueIndexEntryUpdate.ValueSupplier.constant(value));
    }

    private static PropertyValueSupplier after(LazyValueIndexEntryUpdate.ValueSupplier valueSupplier) {
        return new PropertyValueSupplier(NULL_SUPPLIER, valueSupplier, PropertyValueType.After);
    }

    @Override
    protected PropertyValueSupplier unchanged(Value value) {
        return unchanged(LazyValueIndexEntryUpdate.ValueSupplier.constant(value));
    }

    private static PropertyValueSupplier unchanged(LazyValueIndexEntryUpdate.ValueSupplier valueSupplier) {
        return new PropertyValueSupplier(valueSupplier, valueSupplier, PropertyValueType.UnChanged);
    }

    @Override
    protected PropertyValueSupplier changed(Value before, Value after) {
        return changed(
                LazyValueIndexEntryUpdate.ValueSupplier.constant(before),
                LazyValueIndexEntryUpdate.ValueSupplier.constant(after));
    }

    private static PropertyValueSupplier changed(
            LazyValueIndexEntryUpdate.ValueSupplier before, LazyValueIndexEntryUpdate.ValueSupplier after) {
        return new PropertyValueSupplier(before, after, PropertyValueType.Changed);
    }
}
