/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.storageengine.impl.recordstorage;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.IntPredicate;
import java.util.function.Supplier;

import org.neo4j.cursor.Cursor;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.api.AssertOpen;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.IndexReaderFactory;
import org.neo4j.kernel.impl.api.store.AllIdIterator;
import org.neo4j.kernel.impl.api.store.StoreIteratorRelationshipCursor;
import org.neo4j.kernel.impl.api.store.StoreNodeRelationshipCursor;
import org.neo4j.kernel.impl.api.store.StorePropertyCursor;
import org.neo4j.kernel.impl.api.store.StoreSingleNodeCursor;
import org.neo4j.kernel.impl.api.store.StoreSinglePropertyCursor;
import org.neo4j.kernel.impl.api.store.StoreSingleRelationshipCursor;
import org.neo4j.kernel.impl.locking.Lock;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.AbstractDynamicStore;
import org.neo4j.kernel.impl.store.DynamicArrayStore;
import org.neo4j.kernel.impl.store.DynamicStringStore;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RecordCursor;
import org.neo4j.kernel.impl.store.RecordCursors;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.util.InstanceCache;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;
import org.neo4j.storageengine.api.StorageStatement;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.LabelScanReader;

import static org.neo4j.kernel.impl.store.record.AbstractBaseRecord.NO_ID;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

/**
 * Statement for store layer. This allows for acquisition of cursors on the store data.
 * <p/>
 * The cursors call the release methods, so there is no need for manual release, only
 * closing those cursor.
 * <p/>
 */
public class StoreStatement implements StorageStatement
{
    private final InstanceCache<StoreSingleNodeCursor> singleNodeCursor;
    private final InstanceCache<StoreSingleRelationshipCursor> singleRelationshipCursor;
    private final InstanceCache<StoreIteratorRelationshipCursor> iteratorRelationshipCursor;
    private final InstanceCache<StoreNodeRelationshipCursor> nodeRelationshipsCursor;
    private final InstanceCache<StoreSinglePropertyCursor> singlePropertyCursorCache;
    private final InstanceCache<StorePropertyCursor> propertyCursorCache;

    private final NeoStores neoStores;
    private final NodeStore nodeStore;
    private final RelationshipStore relationshipStore;
    private final RelationshipGroupStore relationshipGroupStore;
    private final PropertyStore propertyStore;

    private final Supplier<IndexReaderFactory> indexReaderFactorySupplier;
    private final RecordCursors recordCursors;
    private final Supplier<LabelScanReader> labelScanStore;
    private final RecordStorageCommandCreationContext commandCreationContext;
    private final DynamicArrayStore propertyArrayStore;
    private final DynamicStringStore propertyStringStore;

    private IndexReaderFactory indexReaderFactory;
    private LabelScanReader labelScanReader;

    private boolean acquired;
    private boolean closed;

    private final Nodes nodes;
    private final Relationships relationships;
    private final Groups groups;
    private final Properties properties;

    public StoreStatement( NeoStores neoStores, Supplier<IndexReaderFactory> indexReaderFactory,
            Supplier<LabelScanReader> labelScanReaderSupplier, LockService lockService,
            RecordStorageCommandCreationContext commandCreationContext )
    {
        this.neoStores = neoStores;
        this.indexReaderFactorySupplier = indexReaderFactory;
        this.labelScanStore = labelScanReaderSupplier;
        this.commandCreationContext = commandCreationContext;

        this.nodeStore = neoStores.getNodeStore();
        this.relationshipStore = neoStores.getRelationshipStore();
        this.relationshipGroupStore = neoStores.getRelationshipGroupStore();
        this.propertyStore = neoStores.getPropertyStore();
        this.propertyArrayStore = propertyStore.getArrayStore();
        this.propertyStringStore = propertyStore.getStringStore();
        this.recordCursors = new RecordCursors( neoStores );

        singleNodeCursor = new InstanceCache<StoreSingleNodeCursor>()
        {
            @Override
            protected StoreSingleNodeCursor create()
            {
                return new StoreSingleNodeCursor( nodeStore.newRecord(), this, recordCursors, lockService );
            }
        };
        singleRelationshipCursor = new InstanceCache<StoreSingleRelationshipCursor>()
        {
            @Override
            protected StoreSingleRelationshipCursor create()
            {
                return new StoreSingleRelationshipCursor( relationshipStore.newRecord(), this, recordCursors,
                        lockService );
            }
        };
        iteratorRelationshipCursor = new InstanceCache<StoreIteratorRelationshipCursor>()
        {
            @Override
            protected StoreIteratorRelationshipCursor create()
            {
                return new StoreIteratorRelationshipCursor( relationshipStore.newRecord(), this, recordCursors,
                        lockService );
            }
        };
        nodeRelationshipsCursor = new InstanceCache<StoreNodeRelationshipCursor>()
        {
            @Override
            protected StoreNodeRelationshipCursor create()
            {
                return new StoreNodeRelationshipCursor( relationshipStore.newRecord(),
                        relationshipGroupStore.newRecord(), this, recordCursors, lockService );
            }
        };

        singlePropertyCursorCache = new InstanceCache<StoreSinglePropertyCursor>()
        {
            @Override
            protected StoreSinglePropertyCursor create()
            {
                return new StoreSinglePropertyCursor( recordCursors, this );
            }
        };
        propertyCursorCache = new InstanceCache<StorePropertyCursor>()
        {
            @Override
            protected StorePropertyCursor create()
            {
                return new StorePropertyCursor( recordCursors, this );
            }
        };

        nodes = new Nodes();
        relationships = new Relationships();
        groups = new Groups();
        properties = new Properties();
    }

    @Override
    public void acquire()
    {
        assert !closed;
        assert !acquired;
        this.acquired = true;
    }

    @Override
    public Cursor<NodeItem> acquireSingleNodeCursor( long nodeId )
    {
        neoStores.assertOpen();
        return singleNodeCursor.get().init( nodeId );
    }

    @Override
    public Cursor<RelationshipItem> acquireSingleRelationshipCursor( long relId )
    {
        neoStores.assertOpen();
        return singleRelationshipCursor.get().init( relId );
    }

    @Override
    public Cursor<RelationshipItem> acquireNodeRelationshipCursor( boolean isDense, long nodeId, long relationshipId,
            Direction direction, IntPredicate relTypeFilter )
    {
        neoStores.assertOpen();
        return nodeRelationshipsCursor.get().init( isDense, relationshipId, nodeId, direction, relTypeFilter );
    }

    @Override
    public Cursor<RelationshipItem> relationshipsGetAllCursor()
    {
        neoStores.assertOpen();
        return iteratorRelationshipCursor.get().init( new AllIdIterator( relationshipStore ) );
    }

    @Override
    public Cursor<PropertyItem> acquirePropertyCursor( long propertyId, Lock lock, AssertOpen assertOpen )
    {
        return propertyCursorCache.get().init( propertyId, lock, assertOpen );
    }

    @Override
    public Cursor<PropertyItem> acquireSinglePropertyCursor( long propertyId, int propertyKeyId, Lock lock,
            AssertOpen assertOpen )
    {
        return singlePropertyCursorCache.get().init( propertyId, propertyKeyId, lock, assertOpen );
    }

    @Override
    public void release()
    {
        assert !closed;
        assert acquired;
        closeSchemaResources();
        acquired = false;
    }

    @Override
    public void close()
    {
        assert !closed;
        closeSchemaResources();
        recordCursors.close();
        commandCreationContext.close();
        closed = true;
    }

    private void closeSchemaResources()
    {
        if ( indexReaderFactory != null )
        {
            indexReaderFactory.close();
            // we can actually keep this object around
        }
        if ( labelScanReader != null )
        {
            labelScanReader.close();
            labelScanReader = null;
        }
    }

    @Override
    public LabelScanReader getLabelScanReader()
    {
        return labelScanReader != null ?
                labelScanReader : (labelScanReader = labelScanStore.get());
    }

    private IndexReaderFactory indexReaderFactory()
    {
        return indexReaderFactory != null ?
                indexReaderFactory : (indexReaderFactory = indexReaderFactorySupplier.get());
    }

    @Override
    public IndexReader getIndexReader( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return indexReaderFactory().newReader( descriptor );
    }

    @Override
    public IndexReader getFreshIndexReader( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return indexReaderFactory().newUnCachedReader( descriptor );
    }

    @Override
    public RecordCursors recordCursors()
    {
        return recordCursors;
    }

    RecordStorageCommandCreationContext getCommandCreationContext()
    {
        return commandCreationContext;
    }

    @Override
    public long reserveNode()
    {
        return commandCreationContext.nextId( StoreType.NODE );
    }

    @Override
    public long reserveRelationship()
    {
        return commandCreationContext.nextId( StoreType.RELATIONSHIP );
    }

    @Override
    public Nodes nodes()
    {
        return nodes;
    }

    @Override
    public Relationships relationships()
    {
        return relationships;
    }

    @Override
    public Groups groups()
    {
        return groups;
    }

    @Override
    public Properties properties()
    {
        return properties;
    }

    class Nodes implements StorageStatement.Nodes
    {
        @Override
        public PageCursor openPageCursor( long reference )
        {
            return nodeStore.openPageCursorForReading( reference );
        }

        @Override
        public void loadRecordByCursor( long reference, NodeRecord nodeRecord, RecordLoad mode, PageCursor cursor )
                throws InvalidRecordException
        {
            nodeStore.getRecordByCursor( reference, nodeRecord, mode, cursor );
        }

        @Override
        public long getHighestPossibleIdInUse()
        {
            return nodeStore.getHighestPossibleIdInUse();
        }

        @Override
        public RecordCursor<DynamicRecord> newLabelCursor()
        {
            return newCursor( nodeStore.getDynamicLabelStore() );
        }
    }

    class Relationships implements StorageStatement.Relationships
    {
        @Override
        public PageCursor openPageCursor( long reference )
        {
            return relationshipStore.openPageCursorForReading( reference );
        }

        @Override
        public void loadRecordByCursor( long reference, RelationshipRecord relationshipRecord, RecordLoad mode,
                PageCursor cursor ) throws InvalidRecordException
        {
            relationshipStore.getRecordByCursor( reference, relationshipRecord, mode, cursor );
        }

        @Override
        public long getHighestPossibleIdInUse()
        {
            return relationshipStore.getHighestPossibleIdInUse();
        }
    }

    class Groups implements StorageStatement.Groups
    {
        @Override
        public PageCursor openPageCursor( long reference )
        {
            return relationshipGroupStore.openPageCursorForReading( reference );
        }

        @Override
        public void loadRecordByCursor( long reference, RelationshipGroupRecord relationshipGroupRecord,
                RecordLoad mode, PageCursor cursor ) throws InvalidRecordException
        {
            relationshipGroupStore.getRecordByCursor( reference, relationshipGroupRecord, mode, cursor );
        }

        @Override
        public long getHighestPossibleIdInUse()
        {
            return relationshipGroupStore.getHighestPossibleIdInUse();
        }
    }

    class Properties implements StorageStatement.Properties
    {
        @Override
        public PageCursor openPageCursor( long reference )
        {
            return propertyStore.openPageCursorForReading( reference );
        }

        @Override
        public void loadRecordByCursor( long reference, PropertyRecord propertyBlocks, RecordLoad mode,
                PageCursor cursor ) throws InvalidRecordException
        {
            propertyStore.getRecordByCursor( reference, propertyBlocks, mode, cursor );
        }

        @Override
        public long getHighestPossibleIdInUse()
        {
            return propertyStore.getHighestPossibleIdInUse();
        }

        @Override
        public PageCursor openStringPageCursor( long reference )
        {
            return propertyStringStore.openPageCursorForReading( reference );
        }

        @Override
        public PageCursor openArrayPageCursor( long reference )
        {
            return propertyArrayStore.openPageCursorForReading( reference );
        }

        @Override
        public ByteBuffer loadString( long reference, ByteBuffer buffer, PageCursor page )
        {
            return readDynamic( propertyStore.getStringStore(), reference, buffer, page );
        }

        @Override
        public ByteBuffer loadArray( long reference, ByteBuffer buffer, PageCursor page )
        {
            return readDynamic( propertyStore.getArrayStore(), reference, buffer, page );
        }
    }

    private static ByteBuffer readDynamic( AbstractDynamicStore store, long reference, ByteBuffer buffer,
            PageCursor page )
    {
        if ( buffer == null )
        {
            buffer = ByteBuffer.allocate( 512 );
        }
        else
        {
            buffer.clear();
        }
        DynamicRecord record = store.newRecord();
        do
        {
            //We need to load forcefully here since otherwise we can have inconsistent reads
            //for properties across blocks, see org.neo4j.graphdb.ConsistentPropertyReadsIT
            store.getRecordByCursor( reference, record, RecordLoad.FORCE, page );
            reference = record.getNextBlock();
            byte[] data = record.getData();
            if ( buffer.remaining() < data.length )
            {
                buffer = grow( buffer, data.length );
            }
            buffer.put( data, 0, data.length );
        }
        while ( reference != NO_ID );
        return buffer;
    }

    private static ByteBuffer grow( ByteBuffer buffer, int required )
    {
        buffer.flip();
        int capacity = buffer.capacity();
        do
        {
            capacity *= 2;
        }
        while ( capacity - buffer.limit() < required );
        return ByteBuffer.allocate( capacity ).order( ByteOrder.LITTLE_ENDIAN ).put( buffer );
    }

    private static <R extends AbstractBaseRecord> RecordCursor<R> newCursor( RecordStore<R> store )
    {
        return store.newRecordCursor( store.newRecord() ).acquire( store.getNumberOfReservedLowIds(), NORMAL );
    }
}
