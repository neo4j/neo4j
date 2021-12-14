/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.internal.batchimport;

import java.util.Arrays;

import org.neo4j.internal.batchimport.DataImporter.Monitor;
import org.neo4j.internal.batchimport.input.InputEntityVisitor;
import org.neo4j.internal.batchimport.store.BatchingNeoStores;
import org.neo4j.internal.batchimport.store.BatchingTokenRepository;
import org.neo4j.internal.id.IdGenerator.Marker;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.CommonAbstractStore;
import org.neo4j.kernel.impl.store.DynamicRecordAllocator;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.StandardDynamicRecordAllocator;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.neo4j.storageengine.util.IdUpdateListener.IGNORE;

/**
 * Abstract class containing logic for importing properties for an entity (node/relationship).
 */
abstract class EntityImporter extends InputEntityVisitor.Adapter
{
    private static final String ENTITY_IMPORTER_TAG = "entityImporter";
    private final BatchingTokenRepository.BatchingPropertyKeyTokenRepository propertyKeyTokenRepository;
    private final PropertyStore propertyStore;
    private final PropertyRecord propertyRecord;
    private final PageCursor propertyUpdateCursor;
    protected final StoreCursors storeCursors;
    protected final StoreCursors tempStoreCursors;
    private PropertyBlock[] propertyBlocks = new PropertyBlock[100];
    private int propertyBlocksCursor;
    private final BatchingIdGetter propertyIds;
    private final BatchingIdGetter stringPropertyIds;
    private final BatchingIdGetter arrayPropertyIds;
    protected final Monitor monitor;
    protected final MemoryTracker memoryTracker;
    private long propertyCount;
    protected int entityPropertyCount; // just for the current entity
    private boolean hasPropertyId;
    private long propertyId;
    private final DynamicRecordAllocator dynamicStringRecordAllocator;
    private final DynamicRecordAllocator dynamicArrayRecordAllocator;
    protected final CursorContext cursorContext;

    EntityImporter( BatchingNeoStores stores, Monitor monitor, PageCacheTracer pageCacheTracer, MemoryTracker memoryTracker )
    {
        this.cursorContext = new CursorContext( pageCacheTracer.createPageCursorTracer( ENTITY_IMPORTER_TAG ) );
        this.storeCursors = new CachedStoreCursors( stores.getNeoStores(), cursorContext );
        this.tempStoreCursors = new CachedStoreCursors( stores.getTemporaryNeoStores(), cursorContext );
        this.propertyStore = stores.getPropertyStore();
        this.propertyKeyTokenRepository = stores.getPropertyKeyRepository();
        this.monitor = monitor;
        this.memoryTracker = memoryTracker;
        for ( int i = 0; i < propertyBlocks.length; i++ )
        {
            propertyBlocks[i] = new PropertyBlock();
        }
        this.propertyRecord = propertyStore.newRecord();
        this.propertyIds = new BatchingIdGetter( propertyStore );
        this.stringPropertyIds = new BatchingIdGetter( propertyStore.getStringStore() );
        this.dynamicStringRecordAllocator = new StandardDynamicRecordAllocator( stringPropertyIds, propertyStore.getStringStore().getRecordDataSize() );
        this.arrayPropertyIds = new BatchingIdGetter( propertyStore.getArrayStore() );
        this.dynamicArrayRecordAllocator = new StandardDynamicRecordAllocator( arrayPropertyIds, propertyStore.getStringStore().getRecordDataSize() );
        this.propertyUpdateCursor = propertyStore.openPageCursorForWriting( 0, cursorContext );
    }

    @Override
    public boolean property( String key, Object value )
    {
        assert !hasPropertyId;
        return property( propertyKeyTokenRepository.getOrCreateId( key ), value );
    }

    @Override
    public boolean property( int propertyKeyId, Object value )
    {
        assert !hasPropertyId;
        encodeProperty( nextPropertyBlock(), propertyKeyId, value );
        entityPropertyCount++;
        return true;
    }

    @Override
    public boolean propertyId( long nextProp )
    {
        assert !hasPropertyId;
        hasPropertyId = true;
        propertyId = nextProp;
        return true;
    }

    @Override
    public void endOfEntity()
    {
        propertyBlocksCursor = 0;
        hasPropertyId = false;
        propertyCount += entityPropertyCount;
        entityPropertyCount = 0;
    }

    private PropertyBlock nextPropertyBlock()
    {
        if ( propertyBlocksCursor == propertyBlocks.length )
        {
            propertyBlocks = Arrays.copyOf( propertyBlocks, propertyBlocksCursor * 2 );
            for ( int i = propertyBlocksCursor; i < propertyBlocks.length; i++ )
            {
                propertyBlocks[i] = new PropertyBlock();
            }
        }
        return propertyBlocks[propertyBlocksCursor++];
    }

    private void encodeProperty( PropertyBlock block, int key, Object property )
    {
        Value value = property instanceof Value ? (Value) property : Values.of( property );
        PropertyStore.encodeValue( block, key, value, dynamicStringRecordAllocator, dynamicArrayRecordAllocator, propertyStore.allowStorePointsAndTemporal(),
                cursorContext, memoryTracker );
    }

    long createAndWritePropertyChain( CursorContext cursorContext )
    {
        if ( hasPropertyId )
        {
            return propertyId;
        }

        if ( propertyBlocksCursor == 0 )
        {
            return Record.NO_NEXT_PROPERTY.longValue();
        }

        PropertyRecord currentRecord = propertyRecord( propertyIds.nextId( cursorContext ) );
        long firstRecordId = currentRecord.getId();
        for ( int i = 0; i < propertyBlocksCursor; i++ )
        {
            PropertyBlock block = propertyBlocks[i];
            if ( currentRecord.size() + block.getSize() > PropertyType.getPayloadSize() )
            {
                // This record is full or couldn't fit this block, write it to property store
                long nextPropertyId = propertyIds.nextId( cursorContext );
                long prevId = currentRecord.getId();
                currentRecord.setNextProp( nextPropertyId );
                propertyStore.updateRecord( currentRecord, IGNORE, propertyUpdateCursor, cursorContext, storeCursors );
                currentRecord = propertyRecord( nextPropertyId );
                currentRecord.setPrevProp( prevId );
            }

            // Add this block, there's room for it
            currentRecord.addPropertyBlock( block );
        }

        if ( currentRecord.size() > 0 )
        {
            propertyStore.updateRecord( currentRecord, IGNORE, propertyUpdateCursor, cursorContext, storeCursors );
        }

        return firstRecordId;
    }

    protected abstract PrimitiveRecord primitiveRecord();

    private PropertyRecord propertyRecord( long nextPropertyId )
    {
        propertyRecord.clear();
        propertyRecord.setInUse( true );
        propertyRecord.setId( nextPropertyId );
        primitiveRecord().setIdTo( propertyRecord );
        propertyRecord.setCreated();
        return propertyRecord;
    }

    @Override
    public void close()
    {
        monitor.propertiesImported( propertyCount );
        propertyUpdateCursor.close();
        storeCursors.close();
        tempStoreCursors.close();
    }

    void freeUnusedIds()
    {
        freeUnusedIds( propertyStore, propertyIds, cursorContext );
        freeUnusedIds( propertyStore.getStringStore(), stringPropertyIds, cursorContext );
        freeUnusedIds( propertyStore.getArrayStore(), arrayPropertyIds, cursorContext );
    }

    static void freeUnusedIds( CommonAbstractStore<?,?> store, BatchingIdGetter idBatch, CursorContext cursorContext )
    {
        // Free unused property ids still in the last pre-allocated batch
        try ( Marker marker = store.getIdGenerator().marker( cursorContext ) )
        {
            idBatch.visitUnused( marker::markDeleted );
        }
    }
}
