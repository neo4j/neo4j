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
import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;

import java.util.Arrays;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.values.storable.Value;

/**
 * Represent events related to property changes due to entity addition, deletion or update.
 * This is of use in populating indexes that might be relevant to label/relType and property combinations.
 */
public final class EntityUpdates extends AbstractEntityUpdates<EntityUpdates.PropertyValue> {
    public static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(EntityUpdates.class);

    @SuppressWarnings("UnusedReturnValue")
    public static final class Builder
            extends AbstractEntityUpdates.Builder<EntityUpdates.PropertyValue, EntityUpdates, Builder> {

        private Builder(EntityUpdates updates) {
            super(updates);
        }
    }

    public static Builder forEntity(long entityId, boolean propertyListIsComplete) {
        return new Builder(new EntityUpdates(entityId, EMPTY_INT_ARRAY, EMPTY_INT_ARRAY, propertyListIsComplete));
    }

    private EntityUpdates(
            long entityId, int[] entityTokensBefore, int[] entityTokensAfter, boolean propertyListComplete) {
        super(entityId, entityTokensBefore, entityTokensAfter, propertyListComplete);
    }

    private Value[] valuesBefore(int[] propertyIds, boolean defaultToNoValue) {
        Value[] values = new Value[propertyIds.length];
        for (int i = 0; i < propertyIds.length; i++) {
            values[i] = knownProperty(propertyIds[i], defaultToNoValue).before;
        }
        return values;
    }

    private Value[] valuesAfter(int[] propertyIds) {
        Value[] values = new Value[propertyIds.length];
        for (int i = 0; i < propertyIds.length; i++) {
            PropertyValue propertyValue = knownProperties.get(propertyIds[i]);
            values[i] = propertyValue == null ? null : propertyValue.after;
        }
        return values;
    }

    @Override
    protected IndexEntryUpdate remove(IndexDescriptor indexKey, int[] propertyIds, boolean defaultToNoValue) {
        return EagerValueIndexEntryUpdate.remove(entityId, indexKey, valuesBefore(propertyIds, defaultToNoValue));
    }

    @Override
    protected IndexEntryUpdate add(IndexDescriptor indexKey, int[] propertyIds) {
        return EagerValueIndexEntryUpdate.add(entityId, indexKey, valuesAfter(propertyIds));
    }

    @Override
    protected IndexEntryUpdate change(IndexDescriptor indexKey, int[] propertyIds, boolean defaultToNoValue) {
        return EagerValueIndexEntryUpdate.change(
                entityId, indexKey, valuesBefore(propertyIds, defaultToNoValue), valuesAfter(propertyIds));
    }

    @Override
    public String toString() {
        StringBuilder result =
                new StringBuilder(getClass().getSimpleName()).append('[').append(entityId);
        result.append(", entityTokensBefore:").append(Arrays.toString(entityTokensBefore));
        result.append(", entityTokensAfter:").append(Arrays.toString(entityTokensAfter));
        knownProperties.forEachKeyValue((key, propertyValue) -> {
            result.append(", ");
            result.append(key);
            result.append(" -> ");
            result.append(propertyValue);
        });
        return result.append(']').toString();
    }

    public record PropertyValue(Value before, Value after, PropertyValueType type) implements PropertyValueInterface {

        @Override
        public boolean hasBefore() {
            return before != null;
        }

        @Override
        public boolean hasAfter() {
            return after != null;
        }

        @Override
        public String toString() {
            return switch (type) {
                case NoValue -> "NoValue";
                case Before -> format("Before(%s)", before);
                case After -> format("After(%s)", after);
                case UnChanged -> format("UnChanged(%s)", after);
                case Changed -> format("Changed(from=%s, to=%s)", before, after);
            };
        }
    }

    private static final PropertyValue NO_VALUE = new PropertyValue(null, null, PropertyValueType.NoValue);

    @Override
    protected PropertyValue noValue() {
        return NO_VALUE;
    }

    @Override
    protected PropertyValue before(Value value) {
        return new PropertyValue(value, null, PropertyValueType.Before);
    }

    @Override
    protected PropertyValue after(Value value) {
        return new PropertyValue(null, value, PropertyValueType.After);
    }

    @Override
    protected PropertyValue unchanged(Value value) {
        return new PropertyValue(value, value, PropertyValueType.UnChanged);
    }

    @Override
    protected PropertyValue changed(Value before, Value after) {
        return new PropertyValue(before, after, PropertyValueType.Changed);
    }
}
