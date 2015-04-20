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
package org.neo4j.kernel.impl.api.state;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.kernel.api.txstate.ReadableTxState;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;
import org.neo4j.kernel.impl.util.register.NeoRegister;
import org.neo4j.kernel.impl.util.register.NeoRegisters;
import org.neo4j.register.Register;

/**
 * This is the cursor a user gets when traversing relationships in a transaction with tx state changes.
 *
 * We wrap a store expand cursor, filter its outputs for removed relationships and inject
 * added ones. The injection is complicated, as we need to tell when the store expander has moved to the next
 * set of inputs (ergo called next on its input cursor). So we give the store cursor a specialized
 * input cursor that tracks this and handles injecting any tx-local relationships.
 *
 * This gets particularly complicated as the store cursor is allowed to call next multiple times on its
 * input cursor before generating output, which it may do to perform batching. To mitigate that, the input
 * cursor we give the store only grants one "next" call at a time, and will always return false on the second
 * call. This way, we can find when the store cursor is exhausted for the current input set. When we're done with the
 * current input, we get the next "real" input from our input cursor, and reset the store cursor to use it.
 */
public class AugmentWithLocalStateExpandCursor implements Cursor
{
    /**
     * This is the input cursor we give the store layer, it grants one next call at a time, but can
     * can be controlled to grant another step when we like.
     */
    private static class StoreInputCursor implements Cursor
    {
        private boolean next = false;

        @Override
        public boolean next()
        {
            if(next)
            {
                next = false;
                return true;
            }
            else
            {
                return false;
            }
        }

        @Override
        public void reset() { }

        @Override
        public void close() { }

        public void makeNextAvailable()
        {
            next = true;
        }
    }

    private enum State
    {
        DELEGATING_TO_STORE,
        INJECTING_LOCAL_RELS
    }

    private final RelationshipVisitor<RuntimeException> neighborFetcher = new RelationshipVisitor<RuntimeException>()
    {
        @Override
        public void visit( long id, int type, long startNode, long endNode ) throws RuntimeException
        {
            relId.write( id );
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

    private final StoreInputCursor storeInputCursor = new StoreInputCursor();
    private final Cursor storeCursor;
    private final Cursor inputCursor;

    private final ReadableTxState txState;

    private final NeoRegister.Node.In nodeId;
    private final Register.Object.In<int[]> relTypes;
    private final Register.Object.In<Direction> expandDirection;

    private final NeoRegister.Relationship.Out relId;
    private final NeoRegister.RelType.Out relType;
    private final Register.Object.Out<Direction> direction;
    private final NeoRegister.Node.Out startNodeId;
    private final NeoRegister.Node.Out neighborNodeId;

    private final NeoRegister.RelationshipRegister relIdFromStore = NeoRegisters.newRelationshipRegister();

    private State state = State.DELEGATING_TO_STORE;
    private PrimitiveLongIterator txLocalRels;

    public AugmentWithLocalStateExpandCursor( StoreReadLayer store, ReadableTxState state, Cursor inputCursor,
                                              NeoRegister.Node.In nodeId,
                                              Register.Object.In<int[]> relTypes,
                                              Register.Object.In<Direction> expandDirection, NeoRegister.Relationship.Out relId,
                                              NeoRegister.RelType.Out relType, Register.Object.Out<Direction> direction,
                                              NeoRegister.Node.Out startNodeId, NeoRegister.Node.Out neighborNodeId  )
    {
        this.txState = state;
        this.inputCursor = inputCursor;
        this.nodeId = nodeId;
        this.relTypes = relTypes;
        this.expandDirection = expandDirection;
        this.relId = relId;
        this.relType = relType;
        this.direction = direction;
        this.startNodeId = startNodeId;
        this.neighborNodeId = neighborNodeId;
        this.storeCursor = store.expand( storeInputCursor, nodeId, relTypes, expandDirection,
                relIdFromStore, relType, direction, startNodeId, neighborNodeId );
    }

    @Override
    public boolean next()
    {
        switch(state)
        {
            case DELEGATING_TO_STORE:
                return nextFromStore();
            case INJECTING_LOCAL_RELS:
                return nextTxLocalRel();
            default:
                throw new ThisShouldNotHappenError( "jake", "Unknown state: " + state );
        }
    }

    private boolean nextTxLocalRel()
    {
        if(txLocalRels.hasNext())
        {
            // Pull out the next row from local state
            long id = txLocalRels.next();
            txState.relationshipVisit( id, neighborFetcher );
            return true;
        }
        else
        {
            // All added relationships have been injected, swap to returning rows from the store
            state = State.DELEGATING_TO_STORE;
            return next();
        }
    }

    private boolean nextFromStore()
    {
        // Get the next row from store, unless that row should not show up from the store
        if( !txState.nodeIsAddedInThisTx( nodeId.read() ))
        {
            while(storeCursor.next())
            {
                if(!txState.relationshipIsDeletedInThisTx(relIdFromStore.read()))
                {
                    relId.write( relIdFromStore.read() );
                    return true;
                }
            }
        }

        // The store cursor is exhausted. Move to the next input.
        if(inputCursor.next())
        {
            // This will allow the next store cursor call to pick up another input row
            storeInputCursor.makeNextAvailable();
            storeCursor.reset();

            // If the next input row has local tx changes, swap to injecting those
            txLocalRels = txState.addedRelationships( nodeId.read(), relTypes.read(), expandDirection.read() );
            if(txLocalRels != null)
            {
                state = State.INJECTING_LOCAL_RELS;
            }

            return next();
        }
        else
        {
            return false;
        }
    }

    @Override
    public void reset()
    {
        // Store cursor will delegate to our storeInputCursor, so we need to manually delegate the reset call to the
        // real input cursor
        storeCursor.reset();
        inputCursor.reset();
    }

    @Override
    public void close()
    {
        // Store cursor will delegate to our storeInputCursor, so we need to manually delegate the close call to the
        // real input cursor
        storeCursor.close();
        inputCursor.close();
    }
}
