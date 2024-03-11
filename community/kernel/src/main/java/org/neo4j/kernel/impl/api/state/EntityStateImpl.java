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
package org.neo4j.kernel.impl.api.state;

import static java.lang.Math.toIntExact;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

import org.eclipse.collections.api.IntIterable;
import org.eclipse.collections.api.map.primitive.LongObjectMap;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.neo4j.collection.factory.CollectionsFactory;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.PropertyKeyValue;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.txstate.EntityState;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

class EntityStateImpl implements EntityState {
    private final long id;
    private MutableLongObjectMap<Value> addedProperties;
    private MutableLongObjectMap<Value> changedProperties;
    private MutableLongSet removedProperties;

    final CollectionsFactory collectionsFactory;
    final MemoryTracker memoryTracker;

    EntityStateImpl(long id, CollectionsFactory collectionsFactory, MemoryTracker memoryTracker) {
        this.id = id;
        this.collectionsFactory = requireNonNull(collectionsFactory);
        this.memoryTracker = requireNonNull(memoryTracker);
    }

    public long getId() {
        return id;
    }

    void clear() {
        if (changedProperties != null) {
            changedProperties.clear();
        }
        if (addedProperties != null) {
            addedProperties.clear();
        }
        if (removedProperties != null) {
            removedProperties.clear();
        }
    }

    void changeProperty(int propertyKeyId, Value value) {
        if (addedProperties != null && addedProperties.containsKey(propertyKeyId)) {
            addedProperties.put(propertyKeyId, value);
            return;
        }

        if (changedProperties == null) {
            changedProperties = collectionsFactory.newObjectMap(memoryTracker);
        }
        changedProperties.put(propertyKeyId, value);

        if (removedProperties != null) {
            removedProperties.remove(propertyKeyId);
        }
    }

    void addProperty(int propertyKeyId, Value value) {
        if (removedProperties != null && removedProperties.remove(propertyKeyId)) {
            // This indicates the user did remove+add as two discrete steps, which should be translated to
            // a single change operation.
            changeProperty(propertyKeyId, value);
            return;
        }
        if (addedProperties == null) {
            addedProperties = collectionsFactory.newObjectMap(memoryTracker);
        }
        addedProperties.put(propertyKeyId, value);
    }

    void removeProperty(int propertyKeyId) {
        if (addedProperties != null && addedProperties.remove(propertyKeyId) != null) {
            return;
        }
        if (removedProperties == null) {
            removedProperties = collectionsFactory.newLongSet(memoryTracker);
        }
        removedProperties.add(propertyKeyId);
        if (changedProperties != null) {
            changedProperties.remove(propertyKeyId);
        }
    }

    @Override
    public Iterable<StorageProperty> addedProperties() {
        return toStorageProperties(addedProperties);
    }

    @Override
    public Iterable<StorageProperty> changedProperties() {
        return toStorageProperties(changedProperties);
    }

    @Override
    public IntIterable removedProperties() {
        return removedProperties == null
                ? IntSets.immutable.empty()
                : removedProperties.asLazy().collectInt(Math::toIntExact);
    }

    @Override
    public Iterable<StorageProperty> addedAndChangedProperties() {
        if (addedProperties == null) {
            return toStorageProperties(changedProperties);
        }
        if (changedProperties == null) {
            return toStorageProperties(addedProperties);
        }
        return Iterables.concat(toStorageProperties(addedProperties), toStorageProperties(changedProperties));
    }

    @Override
    public boolean hasPropertyChanges() {
        return (addedProperties != null && !addedProperties.isEmpty())
                || (removedProperties != null && !removedProperties.isEmpty())
                || (changedProperties != null && !changedProperties.isEmpty());
    }

    @Override
    public boolean isPropertyChangedOrRemoved(int propertyKey) {
        return (removedProperties != null && removedProperties.contains(propertyKey))
                || (changedProperties != null && changedProperties.containsKey(propertyKey));
    }

    @Override
    public boolean isPropertyAdded(int propertyKey) {
        return addedProperties != null && addedProperties.containsKey(propertyKey);
    }

    @Override
    public Value propertyValue(int propertyKey) {
        if (removedProperties != null && removedProperties.contains(propertyKey)) {
            return Values.NO_VALUE;
        }
        if (addedProperties != null) {
            Value addedValue = addedProperties.get(propertyKey);
            if (addedValue != null) {
                return addedValue;
            }
        }
        if (changedProperties != null) {
            return changedProperties.get(propertyKey);
        }
        return null;
    }

    private static Iterable<StorageProperty> toStorageProperties(LongObjectMap<Value> propertyMap) {
        return propertyMap == null
                ? emptyList()
                : propertyMap.keyValuesView().collect(e -> new PropertyKeyValue(toIntExact(e.getOne()), e.getTwo()));
    }
}
