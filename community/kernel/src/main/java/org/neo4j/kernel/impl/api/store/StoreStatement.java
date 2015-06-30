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

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.cursor.LabelCursor;
import org.neo4j.kernel.api.cursor.NodeCursor;
import org.neo4j.kernel.api.cursor.PropertyCursor;
import org.neo4j.kernel.api.cursor.RelationshipCursor;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.util.InstanceCache;

/**
 * Statement for store layer. This allows for acquisition of cursors on the store data.
 * <p/>
 * The cursors call the release methods, so there is no need for manual release, only
 * closing those cursor.
 * <p/>
 * {@link NeoStore} caches one of these per thread, so that they can be reused between statements/transactions.
 */
public class StoreStatement
        implements AutoCloseable
{
    private InstanceCache<StoreSingleNodeCursor> singleNodeCursor;
    private InstanceCache<StoreIteratorNodeCursor> iteratorNodeCursor;
    private InstanceCache<StorePropertyCursor> propertyCursor;
    private InstanceCache<StoreLabelCursor> labelCursor;
    private InstanceCache<StoreSingleRelationshipCursor> singleRelationshipCursor;
    private InstanceCache<StoreNodeRelationshipCursor> nodeRelationshipCursor;
    private InstanceCache<StoreIteratorRelationshipCursor> iteratorRelationshipCursor;

    private NeoStore neoStore;

    public StoreStatement( NeoStore neoStore )
    {
        this.neoStore = neoStore;

        singleNodeCursor = new InstanceCache<StoreSingleNodeCursor>()
        {
            @Override
            protected StoreSingleNodeCursor create()
            {
                return new StoreSingleNodeCursor( new NodeRecord( -1 ), StoreStatement.this.neoStore.getNodeStore(),
                        StoreStatement.this, this );
            }
        };
        iteratorNodeCursor = new InstanceCache<StoreIteratorNodeCursor>()
        {
            @Override
            protected StoreIteratorNodeCursor create()
            {
                return new StoreIteratorNodeCursor( new NodeRecord( -1 ), StoreStatement.this.neoStore.getNodeStore(),
                        StoreStatement.this, this );
            }
        };
        propertyCursor = new InstanceCache<StorePropertyCursor>()
        {
            @Override
            protected StorePropertyCursor create()
            {
                return new StorePropertyCursor( StoreStatement.this.neoStore.getPropertyStore(), this );
            }
        };
        labelCursor = new InstanceCache<StoreLabelCursor>()
        {
            @Override
            protected StoreLabelCursor create()
            {
                return new StoreLabelCursor( this );
            }
        };
        singleRelationshipCursor = new InstanceCache<StoreSingleRelationshipCursor>()
        {
            @Override
            protected StoreSingleRelationshipCursor create()
            {
                return new StoreSingleRelationshipCursor( new RelationshipRecord( -1 ),
                        StoreStatement.this.neoStore.getRelationshipStore(), StoreStatement.this, this );
            }
        };
        nodeRelationshipCursor = new InstanceCache<StoreNodeRelationshipCursor>()
        {
            @Override
            protected StoreNodeRelationshipCursor create()
            {
                return new StoreNodeRelationshipCursor( new RelationshipRecord( -1 ),
                        StoreStatement.this.neoStore.getRelationshipStore(),
                        new RelationshipGroupRecord( -1, -1 ),
                        StoreStatement.this.neoStore.getRelationshipGroupStore(), StoreStatement.this, this );
            }
        };
        iteratorRelationshipCursor = new InstanceCache<StoreIteratorRelationshipCursor>()
        {
            @Override
            protected StoreIteratorRelationshipCursor create()
            {
                return new StoreIteratorRelationshipCursor( new RelationshipRecord( -1 ),
                        StoreStatement.this.neoStore.getRelationshipStore(),
                        StoreStatement.this, this );
            }
        };
    }

    public NodeCursor acquireSingleNodeCursor( long nodeId )
    {
        neoStore.assertOpen();
        return singleNodeCursor.get().init( nodeId );
    }

    public NodeCursor acquireIteratorNodeCursor( PrimitiveLongIterator nodeIdIterator )
    {
        neoStore.assertOpen();
        return iteratorNodeCursor.get().init( nodeIdIterator );
    }

    public PropertyCursor acquirePropertyCursor( long firstPropertyRecordId )
    {
        neoStore.assertOpen();
        return propertyCursor.get().init( firstPropertyRecordId );
    }

    public LabelCursor acquireLabelCursor( long[] labels )
    {
        neoStore.assertOpen();
        return labelCursor.get().init( labels );
    }

    public RelationshipCursor acquireNodeRelationshipCursor( boolean dense,
            long nextRel,
            long id,
            Direction direction,
            int[] relTypes )
    {
        neoStore.assertOpen();
        return nodeRelationshipCursor.get().init( dense, nextRel, id, direction, relTypes );
    }

    public RelationshipCursor acquireSingleRelationshipCursor( long relId )
    {
        neoStore.assertOpen();
        return singleRelationshipCursor.get().init( relId );
    }

    public RelationshipCursor acquireIteratorRelationshipCursor( PrimitiveLongIterator iterator )
    {
        neoStore.assertOpen();
        return iteratorRelationshipCursor.get().init( iterator );
    }

    @Override
    public void close()
    {
    }
}
