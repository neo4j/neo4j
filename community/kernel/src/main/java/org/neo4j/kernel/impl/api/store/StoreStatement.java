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
package org.neo4j.kernel.impl.api.store;

import java.util.function.Supplier;

import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.IndexReaderFactory;
import org.neo4j.kernel.impl.locking.Lock;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.util.InstanceCache;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipGroupItem;
import org.neo4j.storageengine.api.RelationshipItem;
import org.neo4j.storageengine.api.StorageStatement;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.LabelScanReader;
import org.neo4j.storageengine.api.txstate.NodeTransactionStateView;
import org.neo4j.storageengine.api.txstate.PropertyContainerState;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;

/**
 * Statement for store layer. This allows for acquisition of cursors on the store data.
 * <p/>
 * The cursors call the release methods, so there is no need for manual release, only
 * closing those cursor.
 * <p/>
 */
public class StoreStatement implements StorageStatement
{
    protected final InstanceCache<NodeCursor> nodeCursor;
    private final InstanceCache<StoreSingleRelationshipCursor> singleRelationshipCursor;
    private final InstanceCache<StoreIteratorRelationshipCursor> iteratorRelationshipCursor;
    private final InstanceCache<StoreNodeRelationshipCursor> nodeRelationshipsCursor;
    private final InstanceCache<StorePropertyCursor> propertyCursorCache;
    private final InstanceCache<StoreSinglePropertyCursor> singlePropertyCursorCache;
    private final InstanceCache<RelationshipGroupCursor> relationshipGroupCursorCache;
    private final InstanceCache<DenseNodeDegreeCounter> degreeVisitableCache;
    private final NeoStores neoStores;
    private final Supplier<IndexReaderFactory> indexReaderFactorySupplier;
    private final Supplier<LabelScanReader> labelScanStore;

    private IndexReaderFactory indexReaderFactory;
    private LabelScanReader labelScanReader;

    private boolean acquired;
    private boolean closed;

    public StoreStatement( NeoStores neoStores, Supplier<IndexReaderFactory> indexReaderFactory,
            Supplier<LabelScanReader> labelScanReaderSupplier, LockService lockService )
    {
        this.neoStores = neoStores;
        this.indexReaderFactorySupplier = indexReaderFactory;
        this.labelScanStore = labelScanReaderSupplier;

        nodeCursor = new InstanceCache<NodeCursor>()
        {
            @Override
            protected NodeCursor create()
            {
                return new NodeCursor( neoStores.getNodeStore(), this, lockService );
            }
        };
        singleRelationshipCursor = new InstanceCache<StoreSingleRelationshipCursor>()
        {
            @Override
            protected StoreSingleRelationshipCursor create()
            {
                return new StoreSingleRelationshipCursor( neoStores.getRelationshipStore(), this, lockService );
            }
        };
        iteratorRelationshipCursor = new InstanceCache<StoreIteratorRelationshipCursor>()
        {
            @Override
            protected StoreIteratorRelationshipCursor create()
            {
                return new StoreIteratorRelationshipCursor( neoStores.getRelationshipStore(), this, lockService );
            }
        };
        nodeRelationshipsCursor = new InstanceCache<StoreNodeRelationshipCursor>()
        {
            @Override
            protected StoreNodeRelationshipCursor create()
            {
                return new StoreNodeRelationshipCursor( neoStores.getRelationshipStore(),
                        neoStores.getRelationshipGroupStore(), this, lockService );
            }
        };
        propertyCursorCache = new InstanceCache<StorePropertyCursor>()
        {
            @Override
            protected StorePropertyCursor create()
            {
                return new StorePropertyCursor( neoStores.getPropertyStore(), this );
            }
        };
        singlePropertyCursorCache = new InstanceCache<StoreSinglePropertyCursor>()
        {
            @Override
            protected StoreSinglePropertyCursor create()
            {
                return new StoreSinglePropertyCursor( neoStores.getPropertyStore(), this );
            }
        };
        degreeVisitableCache = new InstanceCache<DenseNodeDegreeCounter>()
        {
            @Override
            protected DenseNodeDegreeCounter create()
            {
                return new DenseNodeDegreeCounter( neoStores.getRelationshipStore(), neoStores.getRelationshipGroupStore(),
                        this );
            }
        };
        relationshipGroupCursorCache = new InstanceCache<RelationshipGroupCursor>()
        {
            @Override
            protected RelationshipGroupCursor create()
            {
                return new RelationshipGroupCursor( neoStores.getRelationshipGroupStore(), this );
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
    public Progression parallelNodeScanProgression()
    {
        throw unsupportedOperation();
    }

    private UnsupportedOperationException unsupportedOperation()
    {
        return new UnsupportedOperationException( "This operation is not supported in community edition but only in " +
                "enterprise edition" );
    }

    @Override
    public Cursor<NodeItem> acquireNodeCursor( Progression progression, NodeTransactionStateView stateView )
    {
        neoStores.assertOpen();
        return nodeCursor.get().init( progression, stateView );
    }

    @Override
    public Cursor<RelationshipItem> acquireSingleRelationshipCursor( long relId, ReadableTransactionState state )
    {
        neoStores.assertOpen();
        return singleRelationshipCursor.get().init( relId, state );
    }

    @Override
    public Cursor<RelationshipItem> acquireNodeRelationshipCursor( boolean isDense, long nodeId, long relationshipId,
            Direction direction, int[] relTypes, ReadableTransactionState state )
    {
        neoStores.assertOpen();
        return relTypes == null
               ? nodeRelationshipsCursor.get().init( isDense, relationshipId, nodeId, direction, state )
               : nodeRelationshipsCursor.get().init( isDense, relationshipId, nodeId, direction, relTypes, state );
    }

    @Override
    public Cursor<RelationshipItem> relationshipsGetAllCursor( ReadableTransactionState state )
    {
        neoStores.assertOpen();
        return iteratorRelationshipCursor.get().init( new AllIdIterator( neoStores.getRelationshipStore() ), state );
    }

    @Override
    public Cursor<PropertyItem> acquirePropertyCursor( long propertyId, Lock lock, PropertyContainerState state )
    {
        return propertyCursorCache.get().init( propertyId, lock, state );
    }

    @Override
    public Cursor<PropertyItem> acquireSinglePropertyCursor( long propertyId, int propertyKeyId, Lock lock,
            PropertyContainerState state )
    {
        return singlePropertyCursorCache.get().init( propertyKeyId, propertyId, lock, state );
    }

    @Override
    public Cursor<RelationshipGroupItem> acquireRelationshipGroupCursor( long relationshipGroupId )
    {
        return relationshipGroupCursorCache.get().init( relationshipGroupId );
    }

    @Override
    public NodeDegreeCounter acquireNodeDegreeCounter( long nodeId, long relationshipGroupId )
    {
        return degreeVisitableCache.get().init( nodeId, relationshipGroupId );
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
        nodeCursor.close();
        singleRelationshipCursor.close();
        iteratorRelationshipCursor.close();
        nodeRelationshipsCursor.close();
        propertyCursorCache.close();
        singlePropertyCursorCache.close();
        relationshipGroupCursorCache.close();
        degreeVisitableCache.close();
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
}
