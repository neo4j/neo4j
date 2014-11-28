/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.api;

import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.txstate.ReadableTxState;
import org.neo4j.kernel.api.txstate.RelationshipChangeVisitorAdapter;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;
import org.neo4j.kernel.impl.util.diffsets.DiffSetsVisitor;

class CountsDelta
{
    static long forNodes( final StoreReadLayer store, final ReadableTxState tx, final int labelId )
    {
        if ( labelId != ReadOperations.ANY_LABEL )
        {
            class DeletedNodesWithLabel extends DiffSetsVisitor.Adapter<Long>
            {
                long withLabel;

                @Override
                public void visitRemoved( Long nodeId )
                {
                    try
                    {
                        if ( store.nodeHasLabel( nodeId, labelId ) )
                        {
                            withLabel++;
                        }
                    }
                    catch ( EntityNotFoundException e )
                    {
                        throw new IllegalStateException( "Could not read deleted node!", e );
                    }
                }
            }
            DeletedNodesWithLabel deletedNodes = new DeletedNodesWithLabel();
            tx.addedAndRemovedNodes().accept( deletedNodes );
            return tx.nodesWithLabelChanged( labelId ).delta() - deletedNodes.withLabel;
        }
        else
        {
            return tx.addedAndRemovedNodes().delta();
        }
    }

    static long forRelationships( StoreReadLayer store, ReadableTxState tx, int startLabel, int type, int endLabel )
    {
        if ( startLabel != ReadOperations.ANY_LABEL && endLabel != ReadOperations.ANY_LABEL )
        { // relationship count by start node label and end node label -- (:startLabel)-[:type]->(:endLabel)
            if ( type != ReadOperations.ANY_RELATIONSHIP_TYPE )
            {
                throw unsupported( "(:%d)-[:%d]->(:%d)", startLabel, type, endLabel );
            }
            else
            {
                throw unsupported( "(:%d)-->(:%d)", startLabel, endLabel );
            }
        }
        else if ( startLabel != ReadOperations.ANY_LABEL )
        { // relationship count by start node label -- (:startLabel)-[:type]->()
            return relationships( store, tx, startLabel, Direction.OUTGOING, type );
        }
        else if ( endLabel != ReadOperations.ANY_LABEL )
        { // relationship count by end node label -- (:endLabel)<-[:type]-()
            return relationships( store, tx, endLabel, Direction.INCOMING, type );
        }
        else
        { // relationship count independent of labels -- ()-[:type]->()
            if ( type != ReadOperations.ANY_RELATIONSHIP_TYPE )
            {
                return relationships( store, tx, type );
            }
            else
            {
                return tx.addedAndRemovedRelationships().delta();
            }
        }
    }

    private static long relationships( StoreReadLayer store, ReadableTxState tx, int labelId, Direction dir, int type )
    {
        StoredDegreeOfNodesWithLabelChanges label = new StoredDegreeOfNodesWithLabelChanges( store, tx, type, dir );
        MatchingNodeRelationshipChanges rel = new MatchingNodeRelationshipChanges( store, tx, type, labelId, dir );
        tx.nodesWithLabelChanged( labelId ).accept( label );
        tx.addedAndRemovedRelationships().accept( rel );
        return label.changes + rel.changes;
    }

    private static long relationships( final StoreReadLayer store, final ReadableTxState tx, final int typeId )
    {
        class Changes extends RelationshipChangeVisitorAdapter
        {
            long withType;

            Changes()
            {
                super( store, tx );
            }

            @Override
            protected void visitAddedRelationship( long id, int type, long startNode, long endNode )
            {
                if ( type == typeId )
                {
                    withType++;
                }
            }

            @Override
            protected void visitRemovedRelationship( long id, int type, long startNode, long endNode )
            {
                if ( type == typeId )
                {
                    withType--;
                }
            }
        }
        Changes addedAndRemovedRelationships = new Changes();
        tx.addedAndRemovedRelationships().accept( addedAndRemovedRelationships );
        return addedAndRemovedRelationships.withType;
    }

    private static RuntimeException unsupported( String pattern, Object... parameters )
    {
        return new UnsupportedOperationException( "Cannot compute counts for pattern: " +
                                                  String.format( pattern, parameters ) );
    }

    private static class StoredDegreeOfNodesWithLabelChanges implements DiffSetsVisitor<Long>
    {
        private final StoreReadLayer store;
        private final ReadableTxState tx;
        private final int type;
        private final Direction dir;
        long changes;

        StoredDegreeOfNodesWithLabelChanges( StoreReadLayer store, ReadableTxState tx, int type, Direction dir )
        {
            this.store = store;
            this.tx = tx;
            this.type = type;
            this.dir = dir;
        }

        @Override
        public void visitAdded( Long nodeId )
        {
            if ( !tx.nodeIsAddedInThisTx( nodeId ) )
            {
                changes += degree( nodeId );
            }
        }

        @Override
        public void visitRemoved( Long nodeId )
        {
            changes -= degree( nodeId );
        }

        private long degree( long nodeId )
        {
            try
            {
                if ( type == ReadOperations.ANY_RELATIONSHIP_TYPE )
                {
                    return store.nodeGetDegree( nodeId, dir );
                }
                else
                {
                    return store.nodeGetDegree( nodeId, dir, type );
                }
            }
            catch ( EntityNotFoundException e )
            {
                throw new IllegalStateException( "Existing node with label changes did not exist.", e );
            }
        }
    }

    /** Added and removed relationships with matching label/direction/type */
    private static class MatchingNodeRelationshipChanges extends RelationshipChangeVisitorAdapter
    {
        private final StoreReadLayer store;
        private final ReadableTxState tx;
        private final int type, label;
        private final Direction dir;
        long changes;

        MatchingNodeRelationshipChanges( StoreReadLayer store, ReadableTxState tx, int type, int label, Direction dir )
        {
            super( store, tx );
            this.store = store;
            this.tx = tx;
            this.type = type;
            this.label = label;
            this.dir = dir;
        }

        @Override
        protected void visitAddedRelationship( long relationshipId, int type, long startNode, long endNode )
        {
            if ( include( startNode, type, endNode ) )
            {
                changes++;
            }
        }

        @Override
        protected void visitRemovedRelationship( long relationshipId, int type, long startNode, long endNode )
        {
            if ( include( startNode, type, endNode ) )
            {
                changes--;
            }
        }

        private boolean include( long startNode, int type, long endNode )
        {
            if ( this.type == ReadOperations.ANY_RELATIONSHIP_TYPE || this.type == type )
            {
                if ( dir == Direction.OUTGOING )
                {
                    return hasLabel( startNode, label );
                }
                else if ( dir == Direction.INCOMING )
                {
                    return hasLabel( endNode, label );
                }
            }
            return false;
        }

        private boolean hasLabel( long nodeId, int labelId )
        {
            switch ( tx.labelState( nodeId, labelId ) )
            {
            case ADDED:
                return true;
            case REMOVED:
                return false;
            default:
                if ( tx.nodeIsAddedInThisTx( nodeId ) )
                {
                    return false;
                }
                else
                {
                    try
                    {
                        return store.nodeHasLabel( nodeId, labelId );
                    }
                    catch ( EntityNotFoundException e )
                    {
                        throw new IllegalStateException( "Existing node with relationship changes did not exist.", e );
                    }
                }
            }
        }
    }
}
