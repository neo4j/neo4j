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

import static java.lang.Math.toIntExact;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.GROUP_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.LABEL_TOKEN_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.NODE_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.PROPERTY_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.PROPERTY_KEY_TOKEN_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.RELATIONSHIP_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.REL_TYPE_TOKEN_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.SCHEMA_CURSOR;

import org.neo4j.internal.recordstorage.RecordAccess.Loader;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.cursor.CursorType;
import org.neo4j.storageengine.api.cursor.StoreCursors;

public class Loaders {
    private final RecordStore<NodeRecord> nodeStore;
    private final PropertyStore propertyStore;
    private final RecordStore<RelationshipRecord> relationshipStore;
    private final RecordStore<RelationshipGroupRecord> relationshipGroupStore;
    private final RecordStore<PropertyKeyTokenRecord> propertyKeyTokenStore;
    private final RecordStore<RelationshipTypeTokenRecord> relationshipTypeTokenStore;
    private final RecordStore<LabelTokenRecord> labelTokenStore;
    private final SchemaStore schemaStore;
    private final StoreCursors storeCursors;
    private RecordLoader<NodeRecord, Void> nodeLoader;
    private RecordLoader<PropertyRecord, PrimitiveRecord> propertyLoader;
    private RecordLoader<RelationshipRecord, Void> relationshipLoader;
    private RecordLoader<RelationshipGroupRecord, Integer> relationshipGroupLoader;
    private RecordLoader<SchemaRecord, SchemaRule> schemaRuleLoader;
    private RecordLoader<PropertyKeyTokenRecord, Void> propertyKeyTokenLoader;
    private RecordLoader<LabelTokenRecord, Void> labelTokenLoader;
    private RecordLoader<RelationshipTypeTokenRecord, Void> relationshipTypeTokenLoader;

    public Loaders(NeoStores neoStores, StoreCursors storeCursors) {
        this(
                neoStores.getNodeStore(),
                neoStores.getPropertyStore(),
                neoStores.getRelationshipStore(),
                neoStores.getRelationshipGroupStore(),
                neoStores.getPropertyKeyTokenStore(),
                neoStores.getRelationshipTypeTokenStore(),
                neoStores.getLabelTokenStore(),
                neoStores.getSchemaStore(),
                storeCursors);
    }

    public Loaders(
            RecordStore<NodeRecord> nodeStore,
            PropertyStore propertyStore,
            RecordStore<RelationshipRecord> relationshipStore,
            RecordStore<RelationshipGroupRecord> relationshipGroupStore,
            RecordStore<PropertyKeyTokenRecord> propertyKeyTokenStore,
            RecordStore<RelationshipTypeTokenRecord> relationshipTypeTokenStore,
            RecordStore<LabelTokenRecord> labelTokenStore,
            SchemaStore schemaStore,
            StoreCursors storeCursors) {
        this.nodeStore = nodeStore;
        this.propertyStore = propertyStore;
        this.relationshipStore = relationshipStore;
        this.relationshipGroupStore = relationshipGroupStore;
        this.propertyKeyTokenStore = propertyKeyTokenStore;
        this.relationshipTypeTokenStore = relationshipTypeTokenStore;
        this.labelTokenStore = labelTokenStore;
        this.schemaStore = schemaStore;
        this.storeCursors = storeCursors;
    }

    public Loader<NodeRecord, Void> nodeLoader() {
        if (nodeLoader == null) {
            nodeLoader = nodeLoader(nodeStore, storeCursors);
        }
        return nodeLoader;
    }

    public Loader<PropertyRecord, PrimitiveRecord> propertyLoader() {
        if (propertyLoader == null) {
            propertyLoader = propertyLoader(propertyStore, storeCursors);
        }
        return propertyLoader;
    }

    public Loader<RelationshipRecord, Void> relationshipLoader() {
        if (relationshipLoader == null) {
            relationshipLoader = relationshipLoader(relationshipStore, storeCursors);
        }
        return relationshipLoader;
    }

    public Loader<RelationshipGroupRecord, Integer> relationshipGroupLoader() {
        if (relationshipGroupLoader == null) {
            relationshipGroupLoader = relationshipGroupLoader(relationshipGroupStore, storeCursors);
        }
        return relationshipGroupLoader;
    }

    public Loader<SchemaRecord, SchemaRule> schemaRuleLoader() {
        if (schemaRuleLoader == null) {
            schemaRuleLoader = schemaRuleLoader(schemaStore, storeCursors);
        }
        return schemaRuleLoader;
    }

    public Loader<PropertyKeyTokenRecord, Void> propertyKeyTokenLoader() {
        if (propertyKeyTokenLoader == null) {
            propertyKeyTokenLoader = propertyKeyTokenLoader(propertyKeyTokenStore, storeCursors);
        }
        return propertyKeyTokenLoader;
    }

    public Loader<LabelTokenRecord, Void> labelTokenLoader() {
        if (labelTokenLoader == null) {
            labelTokenLoader = labelTokenLoader(labelTokenStore, storeCursors);
        }
        return labelTokenLoader;
    }

    public Loader<RelationshipTypeTokenRecord, Void> relationshipTypeTokenLoader() {
        if (relationshipTypeTokenLoader == null) {
            relationshipTypeTokenLoader = relationshipTypeTokenLoader(relationshipTypeTokenStore, storeCursors);
        }
        return relationshipTypeTokenLoader;
    }

    public static RecordLoader<NodeRecord, Void> nodeLoader(
            final RecordStore<NodeRecord> store, StoreCursors storeCursors) {
        return new RecordLoader<>(store, storeCursors, NODE_CURSOR, NodeRecord.SHALLOW_SIZE) {
            @Override
            public NodeRecord newUnused(long key, Void additionalData, MemoryTracker memoryTracker) {
                memoryTracker.allocateHeap(NodeRecord.SHALLOW_SIZE);
                return andMarkAsCreated(new NodeRecord(key));
            }

            @Override
            public NodeRecord copy(NodeRecord nodeRecord, MemoryTracker memoryTracker) {
                memoryTracker.allocateHeap(NodeRecord.SHALLOW_SIZE);
                return new NodeRecord(nodeRecord);
            }
        };
    }

    public static RecordLoader<PropertyRecord, PrimitiveRecord> propertyLoader(
            final PropertyStore store, StoreCursors storeCursors) {
        return new RecordLoader<>(store, storeCursors, PROPERTY_CURSOR, PropertyRecord.INITIAL_SIZE) {
            @Override
            public PropertyRecord newUnused(long key, PrimitiveRecord additionalData, MemoryTracker memoryTracker) {
                memoryTracker.allocateHeap(PropertyRecord.INITIAL_SIZE);
                PropertyRecord record = new PropertyRecord(key);
                setOwner(record, additionalData);
                return andMarkAsCreated(record);
            }

            private void setOwner(PropertyRecord record, PrimitiveRecord owner) {
                if (owner != null) {
                    owner.setIdTo(record);
                }
            }

            @Override
            public PropertyRecord load(
                    long key, PrimitiveRecord additionalData, RecordLoad load, MemoryTracker memoryTracker) {
                memoryTracker.allocateHeap(PropertyRecord.INITIAL_SIZE);
                PropertyRecord record = super.load(key, additionalData, load, memoryTracker);
                setOwner(record, additionalData);
                return record;
            }

            @Override
            public PropertyRecord copy(PropertyRecord propertyRecord, MemoryTracker memoryTracker) {
                memoryTracker.allocateHeap(PropertyRecord.INITIAL_SIZE);
                return new PropertyRecord(propertyRecord);
            }
        };
    }

    public static RecordLoader<RelationshipRecord, Void> relationshipLoader(
            final RecordStore<RelationshipRecord> store, StoreCursors storeCursors) {
        return new RecordLoader<>(store, storeCursors, RELATIONSHIP_CURSOR, RelationshipRecord.SHALLOW_SIZE) {
            @Override
            public RelationshipRecord newUnused(long key, Void additionalData, MemoryTracker memoryTracker) {
                memoryTracker.allocateHeap(RelationshipRecord.SHALLOW_SIZE);
                return andMarkAsCreated(new RelationshipRecord(key));
            }

            @Override
            public RelationshipRecord copy(RelationshipRecord relationshipRecord, MemoryTracker memoryTracker) {
                memoryTracker.allocateHeap(RelationshipRecord.SHALLOW_SIZE);
                return new RelationshipRecord(relationshipRecord);
            }
        };
    }

    public static RecordLoader<RelationshipGroupRecord, Integer> relationshipGroupLoader(
            final RecordStore<RelationshipGroupRecord> store, StoreCursors storeCursors) {
        return new RecordLoader<>(store, storeCursors, GROUP_CURSOR, RelationshipGroupRecord.SHALLOW_SIZE) {
            @Override
            public RelationshipGroupRecord newUnused(long key, Integer type, MemoryTracker memoryTracker) {
                memoryTracker.allocateHeap(RelationshipGroupRecord.SHALLOW_SIZE);
                RelationshipGroupRecord record = new RelationshipGroupRecord(key);
                record.setType(type);
                return andMarkAsCreated(record);
            }

            @Override
            public RelationshipGroupRecord copy(RelationshipGroupRecord record, MemoryTracker memoryTracker) {
                memoryTracker.allocateHeap(RelationshipGroupRecord.SHALLOW_SIZE);
                return new RelationshipGroupRecord(record);
            }
        };
    }

    private static RecordLoader<SchemaRecord, SchemaRule> schemaRuleLoader(
            final SchemaStore store, StoreCursors storeCursors) {
        return new RecordLoader<>(store, storeCursors, SCHEMA_CURSOR, SchemaRecord.SHALLOW_SIZE) {
            @Override
            public SchemaRecord newUnused(long key, SchemaRule additionalData, MemoryTracker memoryTracker) {
                memoryTracker.allocateHeap(SchemaRecord.SHALLOW_SIZE);
                return andMarkAsCreated(new SchemaRecord(key));
            }

            @Override
            public SchemaRecord copy(SchemaRecord record, MemoryTracker memoryTracker) {
                memoryTracker.allocateHeap(SchemaRecord.SHALLOW_SIZE);
                return new SchemaRecord(record);
            }
        };
    }

    public static RecordLoader<PropertyKeyTokenRecord, Void> propertyKeyTokenLoader(
            final RecordStore<PropertyKeyTokenRecord> store, StoreCursors storeCursors) {
        return new RecordLoader<>(store, storeCursors, PROPERTY_KEY_TOKEN_CURSOR, PropertyKeyTokenRecord.SHALLOW_SIZE) {
            @Override
            public PropertyKeyTokenRecord newUnused(long key, Void additionalData, MemoryTracker memoryTracker) {
                memoryTracker.allocateHeap(PropertyKeyTokenRecord.SHALLOW_SIZE);
                return andMarkAsCreated(new PropertyKeyTokenRecord(toIntExact(key)));
            }

            @Override
            public PropertyKeyTokenRecord copy(PropertyKeyTokenRecord record, MemoryTracker memoryTracker) {
                memoryTracker.allocateHeap(PropertyKeyTokenRecord.SHALLOW_SIZE);
                return new PropertyKeyTokenRecord(record);
            }
        };
    }

    public static RecordLoader<LabelTokenRecord, Void> labelTokenLoader(
            final RecordStore<LabelTokenRecord> store, StoreCursors storeCursors) {
        return new RecordLoader<>(store, storeCursors, LABEL_TOKEN_CURSOR, LabelTokenRecord.SHALLOW_SIZE) {
            @Override
            public LabelTokenRecord newUnused(long key, Void additionalData, MemoryTracker memoryTracker) {
                memoryTracker.allocateHeap(LabelTokenRecord.SHALLOW_SIZE);
                return andMarkAsCreated(new LabelTokenRecord(toIntExact(key)));
            }

            @Override
            public LabelTokenRecord copy(LabelTokenRecord record, MemoryTracker memoryTracker) {
                memoryTracker.allocateHeap(LabelTokenRecord.SHALLOW_SIZE);
                return new LabelTokenRecord(record);
            }
        };
    }

    public static RecordLoader<RelationshipTypeTokenRecord, Void> relationshipTypeTokenLoader(
            final RecordStore<RelationshipTypeTokenRecord> store, StoreCursors storeCursors) {
        return new RecordLoader<>(
                store, storeCursors, REL_TYPE_TOKEN_CURSOR, RelationshipTypeTokenRecord.SHALLOW_SIZE) {
            @Override
            public RelationshipTypeTokenRecord newUnused(long key, Void additionalData, MemoryTracker memoryTracker) {
                memoryTracker.allocateHeap(RelationshipTypeTokenRecord.SHALLOW_SIZE);
                return andMarkAsCreated(new RelationshipTypeTokenRecord(toIntExact(key)));
            }

            @Override
            public RelationshipTypeTokenRecord copy(RelationshipTypeTokenRecord record, MemoryTracker memoryTracker) {
                memoryTracker.allocateHeap(RelationshipTypeTokenRecord.SHALLOW_SIZE);
                return new RelationshipTypeTokenRecord(record);
            }
        };
    }

    protected static <RECORD extends AbstractBaseRecord> RECORD andMarkAsCreated(RECORD record) {
        record.setCreated();
        return record;
    }

    private abstract static class RecordLoader<R extends AbstractBaseRecord, A> implements Loader<R, A> {
        private final RecordStore<R> store;
        private PageCursor pageCursor;
        private final StoreCursors storeCursors;
        private final CursorType cursorType;
        private final long recordHeapSize;

        RecordLoader(RecordStore<R> store, StoreCursors storeCursors, CursorType cursorType, long recordHeapSize) {
            this.store = store;
            this.storeCursors = storeCursors;
            this.cursorType = cursorType;
            this.recordHeapSize = recordHeapSize;
        }

        @Override
        public void ensureHeavy(R record, StoreCursors storeCursors, MemoryTracker memoryTracker) {
            store.ensureHeavy(record, storeCursors, memoryTracker);
        }

        @Override
        public R load(long key, A additionalData, RecordLoad load, MemoryTracker memoryTracker) {
            memoryTracker.allocateHeap(recordHeapSize);
            R record = store.newRecord();
            store.getRecordByCursor(key, record, load, getPageCursor(), memoryTracker);
            return record;
        }

        private PageCursor getPageCursor() {
            if (pageCursor == null) {
                pageCursor = storeCursors.readCursor(cursorType);
            }
            return pageCursor;
        }
    }
}
