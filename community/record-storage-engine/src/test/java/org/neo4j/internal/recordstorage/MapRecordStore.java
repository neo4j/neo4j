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
package org.neo4j.internal.recordstorage;

import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.eclipse.collections.api.map.primitive.LongObjectMap;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.impl.factory.primitive.LongObjectMaps;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.ReflectionUtil;

class MapRecordStore implements LockVerificationMonitor.StoreLoader {
    private final MutableLongObjectMap<NodeRecord> nodeStore = LongObjectMaps.mutable.empty();
    private final MutableLongObjectMap<RelationshipRecord> relationshipStore = LongObjectMaps.mutable.empty();
    private final MutableLongObjectMap<RelationshipGroupRecord> groupStore = LongObjectMaps.mutable.empty();

    RecordAccessSet newRecordChanges(RecordAccess.LoadMonitor loadMonitor, Monitor monitor) {
        return new RecordChangeSet(
                loader(nodeStore, (id, ignore) -> new NodeRecord(id), monitor::nodeWasRead),
                noLoader(),
                loader(relationshipStore, (id, ignore) -> new RelationshipRecord(id), monitor::relationshipWasRead),
                loader(
                        groupStore,
                        (id, type) -> withType(new RelationshipGroupRecord(id), type),
                        monitor::relationshipGroupWasRead),
                noLoader(),
                noLoader(),
                noLoader(),
                noLoader(),
                EmptyMemoryTracker.INSTANCE,
                loadMonitor,
                StoreCursors.NULL);
    }

    void write(NodeRecord record) {
        nodeStore.put(record.getId(), record);
    }

    void write(RelationshipRecord record) {
        relationshipStore.put(record.getId(), record);
    }

    void write(RelationshipGroupRecord record) {
        RelationshipGroupRecord copy = new RelationshipGroupRecord(record);
        // This is a special thing where we know that the prev pointer isn't actually persisted and so that fact is
        // mimiced here
        copy.setPrev(-1);
        groupStore.put(record.getId(), copy);
    }

    @Override
    public NodeRecord loadNode(long id) {
        return loadRecord(id, nodeStore, ignore -> {});
    }

    @Override
    public RelationshipRecord loadRelationship(long id) {
        return loadRecord(id, relationshipStore, ignore -> {});
    }

    @Override
    public RelationshipGroupRecord loadRelationshipGroup(long id) {
        return loadRecord(id, groupStore, ignore -> {});
    }

    @Override
    public PropertyRecord loadProperty(long id) {
        return null;
    }

    @Override
    public SchemaRule loadSchema(long id) {
        return null;
    }

    @Override
    public SchemaRecord loadSchemaRecord(long id) {
        return null;
    }

    Stream<RelationshipGroupRecord> loadRelationshipGroups() {
        return groupStore.values().stream();
    }

    private static RelationshipGroupRecord withType(RelationshipGroupRecord groupRecord, int type) {
        groupRecord.setType(type);
        return groupRecord;
    }

    private static <T extends AbstractBaseRecord> T loadRecord(long key, LongObjectMap<T> store, Consumer<T> monitor) {
        T record = store.get(key);
        monitor.accept(record);
        assert record != null : "Record " + key + " doesn't exist";
        return ReflectionUtil.callCopyConstructor(record);
    }

    private static <T extends AbstractBaseRecord, R> RecordAccess.Loader<T, R> noLoader() {
        return loader(
                LongObjectMaps.mutable.empty(),
                (id, ignore) -> {
                    throw new IllegalStateException("Should not be needed");
                },
                record -> {});
    }

    private static <T extends AbstractBaseRecord, R> RecordAccess.Loader<T, R> loader(
            MutableLongObjectMap<T> store, BiFunction<Long, R, T> factory, Consumer<T> monitor) {
        return new RecordAccess.Loader<>() {
            @Override
            public T newUnused(long key, R additionalData, MemoryTracker memoryTracker) {
                T record = factory.apply(key, additionalData);
                record.setCreated();
                return record;
            }

            @Override
            public T load(long key, R additionalData, RecordLoad load, MemoryTracker memoryTracker) {
                return loadRecord(key, store, monitor);
            }

            @Override
            public void ensureHeavy(T record, StoreCursors storeCursors, MemoryTracker memoryTracker) {
                // ignore
            }

            @Override
            public T copy(T record, MemoryTracker memoryTracker) {
                return ReflectionUtil.callCopyConstructor(record);
            }
        };
    }

    RelationshipChainVisitor relationshipChainVisitor() {
        return new RelationshipChainVisitor(this::loadNode, this::loadRelationship, this::loadRelationshipGroup);
    }

    public interface Monitor {
        default void nodeWasRead(NodeRecord record) {}

        default void relationshipWasRead(RelationshipRecord record) {}

        default void relationshipGroupWasRead(RelationshipGroupRecord record) {}

        Monitor NULL = new Monitor() {};
    }
}
