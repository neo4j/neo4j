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
package org.neo4j.internal.batchimport;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import org.eclipse.collections.api.factory.primitive.IntObjectMaps;
import org.eclipse.collections.api.factory.primitive.IntSets;
import org.eclipse.collections.api.map.primitive.IntObjectMap;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.neo4j.batchimport.api.input.ApplicationMode;
import org.neo4j.storageengine.api.EagerValueIndexEntryUpdate;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.values.storable.Value;

public interface SchemaMonitor extends AutoCloseable {
    ExistingPropertyKeysLookup NO_EXISTING_PROPERTY_KEYS_LOOKUP = (entityId, keysToLookup) -> IntSets.immutable.empty();

    ViolationVisitor NO_VIOLATION_VISITOR = (entity, constraintDescription) -> {};

    SchemaMonitor NO_MONITOR = new SchemaMonitor() {
        @Override
        public boolean handle(
                Entity entity,
                ExistingPropertyKeysLookup existingPropertyKeysLookup,
                ViolationVisitor violationVisitor,
                UniquenessIndexUpdatesListener uniquenessIndexUpdatesListener) {
            return true;
        }

        @Override
        public void indexUpdate(IndexEntryUpdate indexUpdate) {}

        @Override
        public boolean directIndexUpdate(IndexEntryUpdate indexUpdate) {
            return true;
        }

        @Override
        public boolean checkUniqueness(EagerValueIndexEntryUpdate[] checks) {
            return true;
        }

        @Override
        public void close() {}
    };

    UniquenessIndexUpdatesListener EMPTY_UNIQUENESS_UPDATES_LISTENER = new UniquenessIndexUpdatesListener() {
        @Override
        public boolean shouldApply(EagerValueIndexEntryUpdate update) {
            return true;
        }

        @Override
        public void updates(List<EagerValueIndexEntryUpdate> updates) {}

        @Override
        public void endEntity() {}
    };

    boolean handle(
            Entity entity,
            ExistingPropertyKeysLookup existingPropertyKeysLookup,
            ViolationVisitor violationVisitor,
            UniquenessIndexUpdatesListener uniquenessIndexUpdatesListener);

    default boolean[] handle(
            Entity[] entities,
            ExistingPropertyKeysLookup existingPropertyKeysLookup,
            ViolationVisitor violationVisitor,
            UniquenessIndexUpdatesListener uniquenessIndexUpdatesListener) {
        boolean[] result = new boolean[entities.length];
        for (int i = 0; i < entities.length; i++) {
            var entity = entities[i];
            result[i] = entity != null
                    && handle(entity, existingPropertyKeysLookup, violationVisitor, uniquenessIndexUpdatesListener);
        }
        return result;
    }

    /**
     * Used when there's something figuring out relevant index updates for an updated entity,
     * for updated entities where existing data is also read. These index updates can be batched and even
     * deferred to much later.
     * @param indexUpdate index update to apply.
     */
    void indexUpdate(IndexEntryUpdate indexUpdate);

    /**
     * Applies an index update, typically used for uniqueness indexes. Returns {@code true} if the index update
     * could be applied (didn't violate uniqueness), otherwise {@code false}.
     * @param indexUpdate index update to apply directly.
     * @return {@code true} if update could be applied w/o violating uniqueness.
     */
    boolean directIndexUpdate(IndexEntryUpdate indexUpdate);

    /**
     * Checks whether applying these updates would potentially violate any uniqueness constraint.
     * @param checks index updates to check.
     * @return {@code true} if no checks would violate a uniqueness constraint, otherwise {@code false}.
     */
    boolean checkUniqueness(EagerValueIndexEntryUpdate[] checks);

    @Override
    void close();

    interface ViolationVisitor {
        void accept(Entity entity, String constraintDescription);
    }

    interface ExistingPropertyKeysLookup {
        IntSet lookupPropertyKeys(long entityId, IntSet keysToLookup);
    }

    class Entity implements Serializable {
        public final transient Object inputId;
        public long entityId;
        public final List<StorageProperty> properties;
        public final List<StorageProperty> identifyingProperties;
        public final transient ByteBuffer encodedProperties;
        public final transient boolean encodedPropertiesOffloaded;
        public final IntSet removedProperties;
        public final IntSet existingEntityTokens;
        public final IntSet entityTokens;
        public final IntSet removedEntityTokens;
        public final ApplicationMode mode;
        public final transient String sourceDescription;
        public final transient long lineNumber;

        private transient IntObjectMap<Value> propertiesMap;
        private transient int[] sortedEntityTokens;
        private transient int[] sortedExistingEntityTokens;
        private transient IntSet identifierPropertyKeys;

        public Entity(
                Object inputId,
                long entityId,
                List<StorageProperty> properties,
                List<StorageProperty> identifyingProperties,
                ByteBuffer encodedProperties,
                boolean encodedPropertiesOffloaded,
                IntSet removedProperties,
                IntSet existingEntityTokens,
                IntSet entityTokens,
                IntSet removedEntityTokens,
                ApplicationMode mode,
                String sourceDescription,
                long lineNumber) {
            this.inputId = inputId;
            this.entityId = entityId;
            this.properties = properties;
            this.identifyingProperties = identifyingProperties;
            this.encodedProperties = encodedProperties;
            this.encodedPropertiesOffloaded = encodedPropertiesOffloaded;
            this.removedProperties = removedProperties;
            this.existingEntityTokens = existingEntityTokens;
            this.entityTokens = entityTokens;
            this.removedEntityTokens = removedEntityTokens;
            this.mode = mode;
            this.sourceDescription = sourceDescription;
            this.lineNumber = lineNumber;
        }

        public IntObjectMap<Value> propertiesMap() {
            if (propertiesMap == null) {
                var map = IntObjectMaps.mutable.<Value>ofInitialCapacity(properties.size());
                for (var property : properties) {
                    map.put(property.propertyKeyId(), property.value());
                }
                propertiesMap = map;
            }
            return propertiesMap;
        }

        int[] sortedEntityTokens() {
            if (sortedEntityTokens == null) {
                sortedEntityTokens = entityTokens.toSortedArray();
            }
            return sortedEntityTokens;
        }

        int[] sortedExistingEntityTokens() {
            if (sortedExistingEntityTokens == null) {
                sortedExistingEntityTokens = existingEntityTokens.toSortedArray();
            }
            return sortedExistingEntityTokens;
        }

        IntSet identifierPropertyKeys() {
            if (identifierPropertyKeys == null) {
                var keys = IntSets.mutable.withInitialCapacity(identifyingProperties.size());
                for (var identifyingProperty : identifyingProperties) {
                    keys.add(identifyingProperty.propertyKeyId());
                }
                identifierPropertyKeys = keys;
            }
            return identifierPropertyKeys;
        }

        public Entity newWithEntityId(long entityId) {
            return new Entity(
                    inputId,
                    entityId,
                    properties,
                    identifyingProperties,
                    encodedProperties,
                    encodedPropertiesOffloaded,
                    removedProperties,
                    existingEntityTokens,
                    entityTokens,
                    removedEntityTokens,
                    mode,
                    sourceDescription,
                    lineNumber);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Entity entity)) return false;
            return entityId == entity.entityId
                    && Objects.equals(properties, entity.properties)
                    && Objects.equals(identifyingProperties, entity.identifyingProperties)
                    && Objects.equals(removedProperties, entity.removedProperties)
                    && Objects.equals(existingEntityTokens, entity.existingEntityTokens)
                    && Objects.equals(entityTokens, entity.entityTokens)
                    && Objects.equals(removedEntityTokens, entity.removedEntityTokens)
                    && mode == entity.mode;
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    entityId,
                    properties,
                    identifyingProperties,
                    removedProperties,
                    existingEntityTokens,
                    entityTokens,
                    removedEntityTokens,
                    mode);
        }
    }

    class Relationship extends Entity {
        public final long startNodeId;
        public final long endNodeId;
        public final transient Object startId;
        public final transient Object endId;

        public Relationship(
                long entityId,
                List<StorageProperty> properties,
                List<StorageProperty> identifyingProperties,
                ByteBuffer encodedProperties,
                boolean encodedPropertiesOffloaded,
                IntSet removedProperties,
                IntSet existingEntityTokens,
                IntSet entityTokens,
                IntSet removedEntityTokens,
                ApplicationMode mode,
                long startNodeId,
                long endNodeId,
                Object startId,
                Object endId,
                String sourceDescription,
                long lineNumber) {
            super(
                    null,
                    entityId,
                    properties,
                    identifyingProperties,
                    encodedProperties,
                    encodedPropertiesOffloaded,
                    removedProperties,
                    existingEntityTokens,
                    entityTokens,
                    removedEntityTokens,
                    mode,
                    sourceDescription,
                    lineNumber);
            this.startNodeId = startNodeId;
            this.endNodeId = endNodeId;
            this.startId = startId;
            this.endId = endId;
        }

        public int relationshipType() {
            return entityTokens.intIterator().next();
        }

        @Override
        public Relationship newWithEntityId(long relationshipId) {
            return new Relationship(
                    relationshipId,
                    properties,
                    identifyingProperties,
                    encodedProperties,
                    encodedPropertiesOffloaded,
                    removedProperties,
                    existingEntityTokens,
                    entityTokens,
                    removedEntityTokens,
                    mode,
                    startNodeId,
                    endNodeId,
                    startId,
                    endId,
                    sourceDescription,
                    lineNumber);
        }

        public Relationship newWithEntityIds(long relationshipId, long startNodeId, long endNodeId) {
            return new Relationship(
                    relationshipId,
                    properties,
                    identifyingProperties,
                    encodedProperties,
                    encodedPropertiesOffloaded,
                    removedProperties,
                    existingEntityTokens,
                    entityTokens,
                    removedEntityTokens,
                    mode,
                    startNodeId,
                    endNodeId,
                    startId,
                    endId,
                    sourceDescription,
                    lineNumber);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Relationship that)) return false;
            if (!super.equals(o)) return false;
            return startNodeId == that.startNodeId && endNodeId == that.endNodeId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), startNodeId, endNodeId);
        }
    }

    interface UniquenessIndexUpdatesListener {
        boolean shouldApply(EagerValueIndexEntryUpdate update);

        void updates(List<EagerValueIndexEntryUpdate> updates);

        void endEntity();
    }
}
