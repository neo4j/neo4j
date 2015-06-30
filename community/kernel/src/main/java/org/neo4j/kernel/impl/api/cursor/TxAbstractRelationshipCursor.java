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
package org.neo4j.kernel.impl.api.cursor;

import org.neo4j.function.Consumer;
import org.neo4j.kernel.api.cursor.PropertyCursor;
import org.neo4j.kernel.api.cursor.RelationshipCursor;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.state.RelationshipState;

/**
 * Overlays transaction state on a {@link RelationshipCursor}.
 */
public abstract class TxAbstractRelationshipCursor implements RelationshipCursor, RelationshipVisitor<RuntimeException>
{
    protected final TransactionState state;
    private final Consumer<TxAbstractRelationshipCursor> instanceCache;

    protected RelationshipCursor cursor;

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

    public TxAbstractRelationshipCursor init( RelationshipCursor cursor )
    {
        this.cursor = cursor;
        return this;
    }

    @Override
    public long getId()
    {
        if (id == -1)
            throw new IllegalStateException(  );

        return id;
    }

    @Override
    public int getType()
    {
        if (id == -1)
            throw new IllegalStateException(  );

        return type;
    }

    @Override
    public long getStartNode()
    {
        if (id == -1)
            throw new IllegalStateException(  );

        return startNodeId;
    }

    @Override
    public long getEndNode()
    {
        if (id == -1)
            throw new IllegalStateException(  );

        return endNodeId;
    }

    @Override
    public long getOtherNode( long nodeId )
    {
        return startNodeId == nodeId ? endNodeId : startNodeId;
    }

    @Override
    public PropertyCursor properties()
    {
        return state.augmentPropertyCursor(
                relationshipIsAddedInThisTx ? PropertyCursor.EMPTY : cursor.properties(), relationshipState );
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
}
