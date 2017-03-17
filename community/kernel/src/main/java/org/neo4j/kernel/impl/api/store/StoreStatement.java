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

import java.io.IOException;
import java.util.function.Supplier;

import org.neo4j.cursor.Cursor;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.DegreeVisitor;
import org.neo4j.kernel.impl.api.IndexReaderFactory;
import org.neo4j.kernel.impl.locking.Lock;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.CommonAbstractStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RecordCursors;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.util.InstanceCache;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;
import org.neo4j.storageengine.api.StorageStatement;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.LabelScanReader;
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
    private final InstanceCache<NodeCursor> nodeCursor;
    private final InstanceCache<StoreSingleRelationshipCursor> singleRelationshipCursor;
    private final InstanceCache<StoreIteratorRelationshipCursor> iteratorRelationshipCursor;
    private final InstanceCache<StoreNodeRelationshipCursor> nodeRelationshipsCursor;
    private final InstanceCache<StorePropertyCursor> propertyCursorCache;
    private final InstanceCache<StoreSinglePropertyCursor> singlePropertyCursorCache;
    private final InstanceCache<DegreeVisitable> degreeVisitableCache;
    private final NeoStores neoStores;
    private final Supplier<IndexReaderFactory> indexReaderFactorySupplier;
    private final Supplier<LabelScanReader> labelScanStore;
    private final RecordCursors recordCursors;

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
        this.recordCursors = new RecordCursors( neoStores );

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
        degreeVisitableCache = new InstanceCache<DegreeVisitable>()
        {
            @Override
            protected DegreeVisitable create()
            {
                return new DegreeVisitable( neoStores.getRelationshipStore(), neoStores.getRelationshipGroupStore(),
                        this );
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
    public Cursor<NodeItem> acquireNodeCursor( ReadableTransactionState state )
    {
        neoStores.assertOpen();
        return nodeCursor.get().init( new AllNodeProgression( neoStores.getNodeStore() ), state );
    }

    @Override
    public Cursor<NodeItem> acquireSingleNodeCursor( long nodeId, ReadableTransactionState state )
    {
        neoStores.assertOpen();
        return nodeCursor.get().init( new SingleNodeProgression( nodeId ), state );
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
    public DegreeVisitor.Visitable acquireDenseNodeDegreeCounter( long nodeId, long groupId )
    {
        return degreeVisitableCache.get().init( nodeId, groupId );
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
        degreeVisitableCache.close();
        recordCursors.close();
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

    public static <RECORD extends AbstractBaseRecord, STORE extends CommonAbstractStore<RECORD,?>> RECORD read( long id,
            STORE store, RECORD record, RecordLoad mode, PageCursor cursor )
    {
        try
        {
            record.clear();
            store.readIntoRecord( id, record, mode, cursor );
            return record;
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }
}
