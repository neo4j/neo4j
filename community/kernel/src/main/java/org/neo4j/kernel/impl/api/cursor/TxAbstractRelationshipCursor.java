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
package org.neo4j.kernel.impl.api.cursor;

import java.util.function.Consumer;

import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.locking.Lock;
import org.neo4j.storageengine.api.RelationshipItem;
import org.neo4j.storageengine.api.txstate.RelationshipState;

import static org.neo4j.kernel.impl.locking.LockService.NO_LOCK;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_PROPERTY;

/**
 * Overlays transaction state on a {@link RelationshipItem} cursor.
 */
public abstract class TxAbstractRelationshipCursor
        implements Cursor<RelationshipItem>, RelationshipVisitor<RuntimeException>, RelationshipItem
{
    protected final TransactionState state;
    private final Consumer<TxAbstractRelationshipCursor> instanceCache;

    protected Cursor<RelationshipItem> cursor;

    private long id = StatementConstants.NO_SUCH_RELATIONSHIP;
    private int type;
    private long startNodeId;
    private long endNodeId;

    RelationshipState relationshipState;
    protected boolean relationshipIsAddedInThisTx;

    TxAbstractRelationshipCursor( TransactionState state, Consumer<TxAbstractRelationshipCursor> instanceCache )
    {
        this.state = state;
        this.instanceCache = instanceCache;
    }

    public TxAbstractRelationshipCursor init( Cursor<RelationshipItem> cursor )
    {
        this.cursor = cursor;
        return this;
    }

    @Override
    public RelationshipItem get()
    {
        if ( id == StatementConstants.NO_SUCH_RELATIONSHIP )
        {
            throw new IllegalStateException();
        }

        return this;
    }

    @Override
    public void visit( long relId, int type, long startNode, long endNode ) throws RuntimeException
    {
        this.id = relId;
        this.type = type;
        this.startNodeId = startNode;
        this.endNodeId = endNode;
    }

    @Override
    public void close()
    {
        cursor.close();
        cursor = null;
        relationshipState = null;
        instanceCache.accept( this );
    }

    @Override
    public long id()
    {
        return id;
    }

    @Override
    public int type()
    {
        return type;
    }

    @Override
    public long startNode()
    {
        return startNodeId;
    }

    @Override
    public long endNode()
    {
        return endNodeId;
    }

    @Override
    public long otherNode( long nodeId )
    {
        return startNodeId == nodeId ? endNodeId : startNodeId;
    }

    @Override
    public long nextPropertyId()
    {
        return relationshipIsAddedInThisTx ? NO_NEXT_PROPERTY.longValue() : cursor.get().nextPropertyId();
    }

    @Override
    public Lock lock()
    {
        return relationshipIsAddedInThisTx ? NO_LOCK : cursor.get().lock();
    }
}
