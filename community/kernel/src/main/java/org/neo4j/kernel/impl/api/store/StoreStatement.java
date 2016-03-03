/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.IndexReaderFactory;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.CommonAbstractStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.util.InstanceCache;
import org.neo4j.storageengine.api.NodeItem;
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
 * {@link NeoStores} caches one of these per thread, so that they can be reused between statements/transactions.
 */
public class StoreStatement implements StorageStatement
{
    private final InstanceCache<StoreSingleNodeCursor> singleNodeCursor;
    private final InstanceCache<StoreIteratorNodeCursor> iteratorNodeCursor;
    private final InstanceCache<StoreSingleRelationshipCursor> singleRelationshipCursor;
    private final InstanceCache<StoreIteratorRelationshipCursor> iteratorRelationshipCursor;
    private final NeoStores neoStores;
    private final NodeStore nodeStore;
    private final RelationshipStore relationshipStore;
    private final Supplier<IndexReaderFactory> indexReaderFactorySupplier;
    private IndexReaderFactory indexReaderFactory;
    private final Supplier<LabelScanReader> labelScanStore;
    private LabelScanReader labelScanReader;
    private boolean closed;

    public StoreStatement( final NeoStores neoStores, final LockService lockService,
            Supplier<IndexReaderFactory> indexReaderFactory,
            Supplier<LabelScanReader> labelScanReaderSupplier )
    {
        this.neoStores = neoStores;
        this.indexReaderFactorySupplier = indexReaderFactory;
        this.labelScanStore = labelScanReaderSupplier;
        this.nodeStore = neoStores.getNodeStore();
        this.relationshipStore = neoStores.getRelationshipStore();

        singleNodeCursor = new InstanceCache<StoreSingleNodeCursor>()
        {
            @Override
            protected StoreSingleNodeCursor create()
            {
                return new StoreSingleNodeCursor( new NodeRecord( -1 ), neoStores, StoreStatement.this, this,
                        lockService );
            }
        };
        iteratorNodeCursor = new InstanceCache<StoreIteratorNodeCursor>()
        {
            @Override
            protected StoreIteratorNodeCursor create()
            {
                return new StoreIteratorNodeCursor( new NodeRecord( -1 ), neoStores, StoreStatement.this, this,
                        lockService );
            }
        };
        singleRelationshipCursor = new InstanceCache<StoreSingleRelationshipCursor>()
        {
            @Override
            protected StoreSingleRelationshipCursor create()
            {
                return new StoreSingleRelationshipCursor( new RelationshipRecord( -1 ),
                        neoStores, StoreStatement.this, this, lockService );
            }
        };
        iteratorRelationshipCursor = new InstanceCache<StoreIteratorRelationshipCursor>()
        {
            @Override
            protected StoreIteratorRelationshipCursor create()
            {
                return new StoreIteratorRelationshipCursor( new RelationshipRecord( -1 ),
                        neoStores, StoreStatement.this, this, lockService );
            }
        };
    }

    @Override
    public void acquire()
    {
        this.closed = false;
    }

    @Override
    public Cursor<NodeItem> acquireSingleNodeCursor( long nodeId )
    {
        neoStores.assertOpen();
        return singleNodeCursor.get().init( nodeId );
    }

    @Override
    public Cursor<NodeItem> acquireIteratorNodeCursor( PrimitiveLongIterator nodeIdIterator )
    {
        neoStores.assertOpen();
        return iteratorNodeCursor.get().init( nodeIdIterator );
    }

    @Override
    public Cursor<RelationshipItem> acquireSingleRelationshipCursor( long relId )
    {
        neoStores.assertOpen();
        return singleRelationshipCursor.get().init( relId );
    }

    @Override
    public Cursor<RelationshipItem> acquireIteratorRelationshipCursor( PrimitiveLongIterator iterator )
    {
        neoStores.assertOpen();
        return iteratorRelationshipCursor.get().init( iterator );
    }

    @Override
    public Cursor<NodeItem> nodesGetAllCursor()
    {
        return acquireIteratorNodeCursor( new AllStoreIdIterator( nodeStore ) );
    }

    @Override
    public Cursor<RelationshipItem> relationshipsGetAllCursor()
    {
        return acquireIteratorRelationshipCursor( new AllStoreIdIterator( relationshipStore ) );
    }

    @Override
    public void close()
    {
        assert !closed;
        if ( indexReaderFactory != null )
        {
            indexReaderFactory.close();
        }
        if ( labelScanReader != null )
        {
            labelScanReader.close();
            labelScanReader = null;
        }
        closed = true;
    }

    private class AllStoreIdIterator extends PrimitiveLongCollections.PrimitiveLongBaseIterator
    {
        private final CommonAbstractStore store;
        private long highId;
        private long currentId;

        public AllStoreIdIterator( CommonAbstractStore store )
        {
            this.store = store;
            highId = store.getHighestPossibleIdInUse();
        }

        @Override
        protected boolean fetchNext()
        {
            while ( true )
            {   // This outer loop is for checking if highId has changed since we started.
                if ( currentId <= highId )
                {
                    try
                    {
                        return next( currentId );
                    }
                    finally
                    {
                        currentId++;
                    }
                }

                long newHighId = store.getHighestPossibleIdInUse();
                if ( newHighId > highId )
                {
                    highId = newHighId;
                }
                else
                {
                    break;
                }
            }
            return false;
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
