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
package org.neo4j.kernel.impl.api;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterable;
import org.neo4j.function.IntPredicate;
import org.neo4j.graphdb.Direction;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.api.EntityType;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.format.Store;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.standard.StandardStore;

/**
 * Low level {@link PrimitiveLongIterable} for iterating over relationship chains, both sparse and dense.
 * Goes directly to {@link RelationshipStore} and {@link RelationshipGroupStore} for loading its data.
 *
 * {@link #iterator() Emits} {@link StoreRelationshipIterator}, which is a {@link RelationshipIterator}
 * with an added {@link StoreRelationshipIterator#recordClone() relationship record accessor}.
 *
 * No record are created when iterating through this iterator and the {@link RelationshipRecord} data can
 * also be accessed using {@link StoreRelationshipIterator#relationshipVisit(long, RelationshipVisitor)}
 * after a successful call to {@link StoreRelationshipIterator#next()}.
 *
 * TODO this is an excellent place to plug in and use {@link StandardStore} with its {@link Store.RecordCursor}
 * to reduce overhead of calling {@link PagedFile#io(long, int)} on every step through the iterator.
 */
public class StoreRelationshipIterable implements PrimitiveLongIterable
{
    private final NeoStore neoStore;
    private final NodeRecord node;
    private final IntPredicate type;
    private final Direction direction;

    public StoreRelationshipIterable( NeoStore neoStore, long nodeId, IntPredicate type, Direction direction )
            throws EntityNotFoundException
    {
        this.neoStore = neoStore;
        this.type = type;
        this.direction = direction;
        this.node = nodeRecord( neoStore, nodeId );
    }

    public static RelationshipIterator iterator( NeoStore neoStore, long nodeId,
                                                 IntPredicate type, Direction direction ) throws EntityNotFoundException
    {
        NodeRecord node = nodeRecord( neoStore, nodeId );
        return iterator( neoStore, node, type, direction );
    }

    private static NodeRecord nodeRecord( NeoStore neoStore, long nodeId ) throws EntityNotFoundException
    {
        NodeRecord node = neoStore.getNodeStore().loadRecord( nodeId, null );
        if ( node == null )
        {
            throw new EntityNotFoundException( EntityType.NODE, nodeId );
        }
        return node;
    }

    public static RelationshipIterator iterator( NeoStore neoStore, NodeRecord node,
                                                 IntPredicate type, Direction direction )
    {
        RelationshipGroupStore groupStore = neoStore.getRelationshipGroupStore();
        RelationshipStore relationshipStore = neoStore.getRelationshipStore();
        if ( node.isDense() )
        {
            return new DenseIterator( node, groupStore, relationshipStore, type, direction );
        }
        return new SparseIterator( node, relationshipStore, type, direction );
    }

    private static long followRelationshipChain( long nodeId, RelationshipRecord relRecord )
    {
        if ( relRecord.getFirstNode() == nodeId )
        {
            return relRecord.getFirstNextRel();
        }
        else if ( relRecord.getSecondNode() == nodeId )
        {
            return relRecord.getSecondNextRel();
        }

        throw new InvalidRecordException( "While loading relationships for Node[" + nodeId +
                "] a Relationship[" + relRecord.getId() + "] was encountered that had startNode: " +
                relRecord.getFirstNode() + " and endNode: " + relRecord.getSecondNode() +
                ", i.e. which had neither start nor end node as the node we're loading relationships for" );
    }

    @Override
    public RelationshipIterator iterator()
    {
        return iterator( neoStore, node, type, direction );
    }

    public static abstract class StoreRelationshipIterator
            extends PrimitiveLongCollections.PrimitiveLongBaseIterator
            implements RelationshipIterator
    {
        protected final RelationshipStore relationshipStore;
        protected final IntPredicate type;
        protected final Direction direction;
        protected final RelationshipRecord relationship = new RelationshipRecord( -1 );

        private StoreRelationshipIterator( RelationshipStore relationshipStore,
                                           IntPredicate type, Direction direction )
        {
            this.relationshipStore = relationshipStore;
            this.type = type;
            this.direction = direction;
        }

        @Override
        public <EXCEPTION extends Exception> boolean relationshipVisit( long relationshipId,
                RelationshipVisitor<EXCEPTION> visitor ) throws EXCEPTION
        {
            visitor.visit( relationship.getId(), relationship.getType(),
                    relationship.getFirstNode(), relationship.getSecondNode() );
            return false;
        }

        protected boolean directionMatches( long nodeId, RelationshipRecord relationship )
        {
            switch ( direction )
            {
            case BOTH: return true;
            case OUTGOING: return relationship.getFirstNode() == nodeId;
            case INCOMING: return relationship.getSecondNode() == nodeId;
            default: throw new IllegalArgumentException( "Unknown direction " + direction );
            }
        }
    }

    private static class SparseIterator extends StoreRelationshipIterator
    {
        private final long nodeId;
        private long nextRelId;

        SparseIterator( NodeRecord nodeRecord, RelationshipStore relationshipStore,
                        IntPredicate type, Direction direction )
        {
            super( relationshipStore, type, direction );
            this.nodeId = nodeRecord.getId();
            this.nextRelId = nodeRecord.getNextRel();
        }

        @Override
        protected boolean fetchNext()
        {
            while ( nextRelId != Record.NO_NEXT_RELATIONSHIP.intValue() )
            {
                relationshipStore.fillRecord( nextRelId, relationship, RecordLoad.NORMAL );
                try
                {
                    // Filter by type and direction
                    if ( type.test( relationship.getType() ) && directionMatches( nodeId, relationship ) )
                    {
                        return next( nextRelId );
                    }
                }
                finally
                {
                    // Follow the relationship pointer to the next relationship
                    nextRelId = followRelationshipChain( nodeId, relationship );
                }
            }
            return false;
        }
    }

    private static class DenseIterator extends StoreRelationshipIterator
    {
        private final long nodeId;
        private final RelationshipGroupStore groupStore;
        private RelationshipGroupRecord groupRecord;
        private int groupChainIndex;
        private long nextRelId;

        DenseIterator( NodeRecord nodeRecord, RelationshipGroupStore groupStore,
                       RelationshipStore relationshipStore, IntPredicate type, Direction direction )
        {
            super( relationshipStore, type, direction );
            this.groupStore = groupStore;
            this.nodeId = nodeRecord.getId();
            // Apparently returns null if !inUse
            this.groupRecord = groupStore.getRecord( nodeRecord.getNextRel() );
            this.nextRelId = nextChainStart();
        }

        private long nextChainStart()
        {
            while ( groupRecord != null )
            {
                if ( type.test( groupRecord.getType() ) )
                {
                    // Go to the next chain (direction) within this group
                    while ( groupChainIndex < GROUP_CHAINS.length )
                    {
                        GroupChain groupChain = GROUP_CHAINS[groupChainIndex++];
                        long chainStart = groupChain.chainStart( groupRecord );
                        if ( chainStart != Record.NO_NEXT_RELATIONSHIP.intValue() &&
                                (direction == Direction.BOTH || groupChain.matchesDirection( direction ) ) )
                        {
                            return chainStart;
                        }
                    }
                }

                // Go to the next group
                groupRecord = groupRecord.getNext() != Record.NO_NEXT_RELATIONSHIP.intValue() ?
                        groupStore.getRecord( groupRecord.getNext() ) : null;
                groupChainIndex = 0;
            }
            return Record.NO_NEXT_RELATIONSHIP.intValue();
        }

        @Override
        protected boolean fetchNext()
        {
            while ( nextRelId != Record.NO_NEXT_RELATIONSHIP.intValue() )
            {
                relationshipStore.fillRecord( nextRelId, relationship, RecordLoad.NORMAL );
                try
                {
                    return next( nextRelId );
                }
                finally
                {
                    // Follow the relationship pointer to the next relationship
                    nextRelId = followRelationshipChain( nodeId, relationship );
                    if ( nextRelId == Record.NO_NEXT_RELATIONSHIP.intValue() )
                    {
                        // End of chain, try the next chain
                        nextRelId = nextChainStart();
                        // Potentially end of all chains here, and that's fine, we'll exit below
                    }
                }
            }
            return false;
        }
    }

    private static enum GroupChain
    {
        OUT
        {
            @Override
            long chainStart( RelationshipGroupRecord groupRecord )
            {
                return groupRecord.getFirstOut();
            }

            @Override
            boolean matchesDirection( Direction direction )
            {
                return direction == Direction.OUTGOING;
            }
        },
        IN
        {
            @Override
            long chainStart( RelationshipGroupRecord groupRecord )
            {
                return groupRecord.getFirstIn();
            }

            @Override
            boolean matchesDirection( Direction direction )
            {
                return direction == Direction.INCOMING;
            }
        },
        LOOP
        {
            @Override
            long chainStart( RelationshipGroupRecord groupRecord )
            {
                return groupRecord.getFirstLoop();
            }

            @Override
            boolean matchesDirection( Direction direction )
            {
                return true;
            }
        };

        abstract long chainStart( RelationshipGroupRecord groupRecord );

        abstract boolean matchesDirection( Direction direction );
    }

    private static final GroupChain[] GROUP_CHAINS = GroupChain.values();
}
