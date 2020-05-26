/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.recordstorage;

import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.StandardDynamicRecordAllocator;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.lock.ResourceLocker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.CommandCreationContext;

import static java.lang.Math.toIntExact;

/**
 * Holds commit data structures for creating records in a {@link NeoStores}.
 */
class RecordStorageCommandCreationContext implements CommandCreationContext
{
    private final NeoStores neoStores;
    private final Loaders loaders;
    private final MemoryTracker memoryTracker;
    private final RelationshipCreator relationshipCreator;
    private final RelationshipDeleter relationshipDeleter;
    private final PropertyCreator propertyCreator;
    private final PropertyDeleter propertyDeleter;
    private final PageCursorTracer cursorTracer;

    RecordStorageCommandCreationContext( NeoStores neoStores, int denseNodeThreshold, PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
    {
        this.cursorTracer = cursorTracer;
        this.neoStores = neoStores;
        this.memoryTracker = memoryTracker;
        this.loaders = new Loaders( neoStores );
        RelationshipGroupGetter relationshipGroupGetter = new RelationshipGroupGetter( neoStores.getRelationshipGroupStore(), cursorTracer );
        this.relationshipCreator = new RelationshipCreator( relationshipGroupGetter, denseNodeThreshold, cursorTracer );
        PropertyTraverser propertyTraverser = new PropertyTraverser( cursorTracer );
        this.propertyDeleter = new PropertyDeleter( propertyTraverser, cursorTracer );
        this.relationshipDeleter = new RelationshipDeleter( relationshipGroupGetter, propertyDeleter, cursorTracer );
        PropertyStore propertyStore = neoStores.getPropertyStore();
        this.propertyCreator = new PropertyCreator(
                new StandardDynamicRecordAllocator( propertyStore.getStringStore(), propertyStore.getStringStore().getRecordDataSize() ),
                new StandardDynamicRecordAllocator( propertyStore.getArrayStore(), propertyStore.getArrayStore().getRecordDataSize() ), propertyStore,
                propertyTraverser, propertyStore.allowStorePointsAndTemporal(), cursorTracer, memoryTracker );
    }

    private long nextId( StoreType storeType )
    {
        return neoStores.getRecordStore( storeType ).nextId( cursorTracer );
    }

    @Override
    public long reserveNode()
    {
        return nextId( StoreType.NODE );
    }

    @Override
    public long reserveRelationship()
    {
        return nextId( StoreType.RELATIONSHIP );
    }

    @Override
    public long reserveSchema()
    {
        return nextId( StoreType.SCHEMA );
    }

    @Override
    public int reserveRelationshipTypeTokenId()
    {
        return toIntExact( neoStores.getRelationshipTypeTokenStore().nextId( cursorTracer ) );
    }

    @Override
    public int reservePropertyKeyTokenId()
    {
        return toIntExact( neoStores.getPropertyKeyTokenStore().nextId( cursorTracer ) );
    }

    @Override
    public int reserveLabelTokenId()
    {
        return toIntExact( neoStores.getLabelTokenStore().nextId( cursorTracer ) );
    }

    @Override
    public void close()
    {
    }

    TransactionRecordState createTransactionRecordState( IntegrityValidator integrityValidator, long lastTransactionIdWhenStarted,
            ResourceLocker locks )
    {
        RecordChangeSet recordChangeSet = new RecordChangeSet( loaders, memoryTracker );
        return new TransactionRecordState( neoStores, integrityValidator,
                recordChangeSet, lastTransactionIdWhenStarted, locks,
                relationshipCreator, relationshipDeleter, propertyCreator, propertyDeleter, cursorTracer, memoryTracker );
    }
}
