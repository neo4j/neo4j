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
import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.util.register.NeoRegister;
import org.neo4j.register.Register;

/**
 * This is a temporary implementation of the store-layer expand cursor. The purpose of this is to be thrown away and
 * replaced by an implementation that works directly against the store files.
 *
 * As it stands, this delegates to a combination of getRels and visitRel in the store layer to perform it's duties.
 */
public class StoreExpandCursor implements Cursor
{
    private final CacheLayer store;
    private final Cursor inputCursor;
    private final NeoRegister.Node.In nodeId;
    private final Register.Object.In<int[]> relTypes;
    private final Register.Object.In<Direction> expandDirection;
    private final NeoRegister.Relationship.Out relId;
    private final NeoRegister.RelType.Out relType;
    private final Register.Object.Out<Direction> direction;
    private final NeoRegister.Node.Out startNodeId;
    private final NeoRegister.Node.Out neighborNodeId;

    private final RelationshipVisitor<RuntimeException> neighborFetcher = new RelationshipVisitor<RuntimeException>()
    {
        @Override
        public void visit( long relId, int type, long startNode, long endNode ) throws RuntimeException
        {
            long origin = nodeId.read();
            if( startNode == endNode)
            {
                startNodeId.write( origin );
                neighborNodeId.write( origin );
                direction.write( Direction.BOTH );
                relType.write( type );
            }
            else if(startNode == origin)
            {
                startNodeId.write( origin );
                neighborNodeId.write( endNode );
                direction.write( Direction.OUTGOING );
                relType.write( type );
            }
            else
            {
                startNodeId.write( origin );
                neighborNodeId.write( startNode );
                direction.write( Direction.INCOMING );
                relType.write( type );
            }
        }
    };

    private PrimitiveLongIterator relIterator;

    public StoreExpandCursor( CacheLayer cacheLayer, Cursor inputCursor,
                              NeoRegister.Node.In nodeId, Register.Object.In<int[]> relTypes,
                              Register.Object.In<Direction> expandDirection, NeoRegister.Relationship.Out relId,
                              NeoRegister.RelType.Out relType, Register.Object.Out<Direction> direction,
                              NeoRegister.Node.Out startNodeId, NeoRegister.Node.Out neighborNodeId )
    {
        this.store = cacheLayer;
        this.inputCursor = inputCursor;
        this.nodeId = nodeId;
        this.relTypes = relTypes;
        this.expandDirection = expandDirection;
        this.relId = relId;
        this.relType = relType;
        this.direction = direction;
        this.startNodeId = startNodeId;
        this.neighborNodeId = neighborNodeId;
    }

    @Override
    public boolean next()
    {
        if(relIterator == null)
        {
            if ( !nextInputNode() )
            {
                return false;
            }
        }

        if(relIterator.hasNext())
        {
            return nextRelationship();
        }
        else
        {
            relIterator = null;
            return next();
        }
    }

    private boolean nextRelationship()
    {
        long next = relIterator.next();
        relId.write( next );
        try
        {
            store.relationshipVisit( next, neighborFetcher );
            return true;
        }
        catch ( EntityNotFoundException e )
        {
            return next();
        }
    }

    private boolean nextInputNode()
    {
        if( inputCursor.next() )
        {
            try
            {
                relIterator = store.nodeListRelationships( nodeId.read(), expandDirection.read(), relTypes.read() );
                return true;
            }
            catch ( EntityNotFoundException e )
            {
                return nextInputNode();
            }
        }
        else
        {
            return false;
        }
    }

    @Override
    public void reset()
    {
        inputCursor.reset();
    }

    @Override
    public void close()
    {
        inputCursor.close();
    }
}
