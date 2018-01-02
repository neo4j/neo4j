/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.cursor;

import org.neo4j.cursor.Cursor;
import org.neo4j.function.Consumer;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.api.cursor.EntityItem;
import org.neo4j.kernel.api.cursor.PropertyItem;
import org.neo4j.kernel.api.cursor.RelationshipItem;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.state.RelationshipState;
import org.neo4j.kernel.impl.util.Cursors;

/**
 * Overlays transaction state on a {@link RelationshipItem} cursor.
 */
public abstract class TxAbstractRelationshipCursor
        extends EntityItem.EntityItemHelper implements Cursor<RelationshipItem>, RelationshipItem,
        RelationshipVisitor<RuntimeException>
{
    protected final TransactionState state;
    private final Consumer<TxAbstractRelationshipCursor> instanceCache;

    protected Cursor<RelationshipItem> cursor;

    private long id;
    private int type;
    private long startNodeId;
    private long endNodeId;

    protected RelationshipState relationshipState;
    protected boolean relationshipIsAddedInThisTx;

    public TxAbstractRelationshipCursor( TransactionState state,
            Consumer<TxAbstractRelationshipCursor> instanceCache )
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
    public Cursor<PropertyItem> properties()
    {
        return state.augmentPropertyCursor(
                relationshipIsAddedInThisTx ? Cursors.<PropertyItem>empty() : cursor.get().properties(),
                relationshipState );
    }

    @Override
    public Cursor<PropertyItem> property( int propertyKeyId )
    {
        return state.augmentSinglePropertyCursor(
                relationshipIsAddedInThisTx ? Cursors.<PropertyItem>empty() : cursor.get().property( propertyKeyId ),
                relationshipState,
                propertyKeyId );
    }
}
