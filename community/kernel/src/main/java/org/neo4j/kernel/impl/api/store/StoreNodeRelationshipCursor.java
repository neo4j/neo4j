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
package org.neo4j.kernel.impl.api.store;

import java.util.function.Consumer;
import java.util.function.IntPredicate;

import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.kernel.impl.store.RecordCursors;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.storageengine.api.Direction;

import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_RELATIONSHIP;
import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;
import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;

/**
 * Cursor over the chain of relationships from one node.
 * <p/>
 * This cursor handles both dense and non-dense nodes as source.
 */
public class StoreNodeRelationshipCursor extends StoreAbstractRelationshipCursor
{
    private final RelationshipGroupRecord groupRecord;
    private final Consumer<StoreNodeRelationshipCursor> instanceCache;
    private final RecordCursors cursors;

    // Reset all this state on init()
    // --------
    private boolean isDense;
    private long relationshipId;
    private long fromNodeId;
    private Direction direction;
    private IntPredicate allowedTypes;
    private int groupChainIndex;
    private boolean end;
    // --------

    public StoreNodeRelationshipCursor( RelationshipRecord relationshipRecord,
            RelationshipGroupRecord groupRecord,
            Consumer<StoreNodeRelationshipCursor> instanceCache,
            RecordCursors cursors,
            LockService lockService )
    {
        super( relationshipRecord, cursors, lockService );
        this.groupRecord = groupRecord;
        this.instanceCache = instanceCache;
        this.cursors = cursors;
    }

    public StoreNodeRelationshipCursor init( boolean isDense,
            long firstRelId,
            long fromNodeId,
            Direction direction,
            IntPredicate allowedTypes )
    {
        this.isDense = isDense;
        this.relationshipId = firstRelId;
        this.fromNodeId = fromNodeId;
        this.direction = direction;
        this.allowedTypes = allowedTypes;
        this.groupChainIndex = 0;
        this.end = false;

        if ( isDense && relationshipId != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            cursors.relationshipGroup().next( firstRelId, groupRecord, FORCE );
            relationshipId = nextChainStart();
        }
        else
        {
            relationshipId = firstRelId;
        }

        return this;
    }

    @Override
    public boolean next()
    {
        while ( relationshipId != NO_NEXT_RELATIONSHIP.intValue() )
        {
            relationshipRecordCursor.next( relationshipId, relationshipRecord, FORCE );

            // If we end up on a relationship record that isn't in use there's a good chance there
            // have been a concurrent transaction deleting this record under our feet. Since we don't
            // reuse relationship ids we can still trust the pointers in this unused record and try
            // to chase a used record down the line.
            try
            {
                // Direction check
                if ( relationshipRecord.inUse() )
                {
                    if ( direction != Direction.BOTH )
                    {
                        switch ( direction )
                        {
                        case INCOMING:
                        {
                            if ( relationshipRecord.getSecondNode() != fromNodeId )
                            {
                                continue;
                            }
                            break;
                        }

                        case OUTGOING:
                        {
                            if ( relationshipRecord.getFirstNode() != fromNodeId )
                            {
                                continue;
                            }
                            break;
                        }

                        default:
                            throw new IllegalStateException( "Unknown direction: " + direction );
                        }
                    }

                    // Type check
                    if ( !allowedTypes.test( relationshipRecord.getType() ) )
                    {
                        continue;
                    }
                    return true;
                }
            }
            finally
            {
                // Pick next relationship
                if ( relationshipRecord.getFirstNode() == fromNodeId )
                {
                    relationshipId = relationshipRecord.getFirstNextRel();
                }
                else if ( relationshipRecord.getSecondNode() == fromNodeId )
                {
                    relationshipId = relationshipRecord.getSecondNextRel();
                }
                else
                {
                    throw new InvalidRecordException(
                            "While loading relationships for Node[" + fromNodeId + "] a Relationship[" +
                                    relationshipRecord.getId() + "] was encountered that had startNode:" + " " +
                                    relationshipRecord.getFirstNode() + " and endNode: " +
                                    relationshipRecord.getSecondNode() + ", i.e. which had neither start nor end node " +
                                    "as the node we're loading relationships for" );
                }

                // If there are no more relationships, and this is from a dense node, then
                // traverse the next group
                if ( relationshipId == NO_NEXT_RELATIONSHIP.intValue() && isDense )
                {
                    relationshipId = nextChainStart();
                }
            }
        }

        return false;
    }

    @Override
    public void close()
    {
        instanceCache.accept( this );
    }

    private long nextChainStart()
    {
        try
        {
            while ( !end )
            {
                // We check inUse flag here since we can actually follow pointers in unused records
                // to guard for and overcome concurrent deletes in the relationship group chain
                if ( groupRecord.inUse() && allowedTypes.test( groupRecord.getType() ) )
                {
                    // Go to the next chain (direction) within this group
                    while ( groupChainIndex < GROUP_CHAINS.length )
                    {
                        GroupChain groupChain = GROUP_CHAINS[groupChainIndex++];
                        long chainStart = groupChain.chainStart( groupRecord );
                        if ( !NULL_REFERENCE.is( chainStart )
                             && (direction == Direction.BOTH || groupChain.matchesDirection( direction )) )
                        {
                            return chainStart;
                        }
                    }
                }
                // Go to the next group
                if ( !NULL_REFERENCE.is( groupRecord.getNext() ) )
                {
                    cursors.relationshipGroup().next( groupRecord.getNext(), groupRecord, FORCE );
                }
                else
                {
                    end = true;
                }
                groupChainIndex = 0;
            }
        }
        catch ( InvalidRecordException e )
        {
            // Ignore - next line will ensure we're finished anyway
        }
        return NULL_REFERENCE.intValue();
    }

    private enum GroupChain
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
