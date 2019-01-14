/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.storageengine.impl.recordstorage;

import java.util.function.IntPredicate;
import java.util.function.Supplier;

import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.api.AssertOpen;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
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
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RecordCursors;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.util.InstanceCache;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;
import org.neo4j.storageengine.api.StorageStatement;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.LabelScanReader;

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

    private IndexReaderFactory indexReaderFactory;
    private LabelScanReader labelScanReader;

    private boolean acquired;
    private boolean closed;

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
    public IndexReader getIndexReader( SchemaIndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return indexReaderFactory().newReader( descriptor );
    }

    @Override
    public IndexReader getFreshIndexReader( SchemaIndexDescriptor descriptor ) throws IndexNotFoundKernelException
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
        return nodeStore;
    }

    @Override
    public Relationships relationships()
    {
        return relationshipStore;
    }

    @Override
    public Groups groups()
    {
        return relationshipGroupStore;
    }

    @Override
    public Properties properties()
    {
        return propertyStore;
    }

    @Override
    public long getGraphPropertyReference()
    {
        return neoStores.getMetaDataStore().getGraphNextProp();
    }
}
