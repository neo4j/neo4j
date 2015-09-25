/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.api.cursor.NodeItem;
import org.neo4j.kernel.api.cursor.RelationshipItem;
import org.neo4j.kernel.impl.store.CommonAbstractStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.util.InstanceCache;

/**
 * Statement for store layer. This allows for acquisition of cursors on the store data.
 * <p/>
 * The cursors call the release methods, so there is no need for manual release, only
 * closing those cursor.
 * <p/>
 * {@link NeoStores} caches one of these per thread, so that they can be reused between statements/transactions.
 */
public class StoreStatement
        implements AutoCloseable
{
    private InstanceCache<StoreSingleNodeCursor> singleNodeCursor;
    private InstanceCache<StoreIteratorNodeCursor> iteratorNodeCursor;
    private InstanceCache<StoreSingleRelationshipCursor> singleRelationshipCursor;
    private InstanceCache<StoreIteratorRelationshipCursor> iteratorRelationshipCursor;

    private NeoStores neoStores;

    public StoreStatement( NeoStores neoStores )
    {
        this.neoStores = neoStores;

        singleNodeCursor = new InstanceCache<StoreSingleNodeCursor>()
        {
            @Override
            protected StoreSingleNodeCursor create()
            {
                return new StoreSingleNodeCursor( new NodeRecord( -1 ), StoreStatement.this.neoStores,
                        StoreStatement.this, this );
            }
        };
        iteratorNodeCursor = new InstanceCache<StoreIteratorNodeCursor>()
        {
            @Override
            protected StoreIteratorNodeCursor create()
            {
                return new StoreIteratorNodeCursor( new NodeRecord( -1 ), StoreStatement.this.neoStores,
                        StoreStatement.this, this );
            }
        };
        singleRelationshipCursor = new InstanceCache<StoreSingleRelationshipCursor>()
        {
            @Override
            protected StoreSingleRelationshipCursor create()
            {
                return new StoreSingleRelationshipCursor( new RelationshipRecord( -1 ),
                        StoreStatement.this.neoStores, StoreStatement.this, this );
            }
        };
        iteratorRelationshipCursor = new InstanceCache<StoreIteratorRelationshipCursor>()
        {
            @Override
            protected StoreIteratorRelationshipCursor create()
            {
                return new StoreIteratorRelationshipCursor( new RelationshipRecord( -1 ),
                        StoreStatement.this.neoStores,
                        StoreStatement.this, this );
            }
        };
    }

    public Cursor<NodeItem> acquireSingleNodeCursor( long nodeId )
    {
        neoStores.assertOpen();
        return singleNodeCursor.get().init( nodeId );
    }

    public Cursor<NodeItem> acquireIteratorNodeCursor( PrimitiveLongIterator nodeIdIterator )
    {
        neoStores.assertOpen();
        return iteratorNodeCursor.get().init( nodeIdIterator );
    }

    public Cursor<RelationshipItem> acquireSingleRelationshipCursor( long relId )
    {
        neoStores.assertOpen();
        return singleRelationshipCursor.get().init( relId );
    }

    public Cursor<RelationshipItem> acquireIteratorRelationshipCursor( PrimitiveLongIterator iterator )
    {
        neoStores.assertOpen();
        return iteratorRelationshipCursor.get().init( iterator );
    }

    public Cursor<NodeItem> nodesGetAllCursor()
    {
        return acquireIteratorNodeCursor( new AllStoreIdIterator( neoStores.getNodeStore() ) );
    }

    public Cursor<RelationshipItem> relationshipsGetAllCursor()
    {
        return acquireIteratorRelationshipCursor( new AllStoreIdIterator( neoStores.getRelationshipStore() ) );
    }

    @Override
    public void close()
    {
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
}
