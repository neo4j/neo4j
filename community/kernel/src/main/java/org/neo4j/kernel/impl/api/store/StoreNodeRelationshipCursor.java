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

import org.neo4j.function.Consumer;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

/**
 * Cursor over the chain of relationships from one node.
 * <p/>
 * This cursor handles both dense and non-dense nodes as source.
 */
public class StoreNodeRelationshipCursor extends StoreAbstractRelationshipCursor
{
    private final RelationshipGroupRecord groupRecordInstance;

    private final RelationshipGroupStore groupStore;
    private final Consumer<StoreNodeRelationshipCursor> instanceCache;

    private boolean isDense;
    private long relationshipId;
    private long fromNodeId;
    private Direction direction;
    private int[] relTypes;
    private RelationshipGroupRecord groupRecord;
    private int groupChainIndex;

    public StoreNodeRelationshipCursor( RelationshipRecord relationshipRecord,
            RelationshipStore relationshipStore,
            RelationshipGroupRecord groupRecord,
            RelationshipGroupStore groupStore,
            StoreStatement storeStatement,
            Consumer<StoreNodeRelationshipCursor> instanceCache )
    {
        super( relationshipRecord, relationshipStore, storeStatement );

        this.groupRecordInstance = groupRecord;
        this.groupStore = groupStore;
        this.instanceCache = instanceCache;
    }

    public StoreNodeRelationshipCursor init( boolean isDense,
            long firstRelId,
            long fromNodeId,
            Direction direction )
    {
        return init( isDense, firstRelId, fromNodeId, direction, null );
    }

    public StoreNodeRelationshipCursor init( boolean isDense,
            long firstRelId,
            long fromNodeId,
            Direction direction,
            int... relTypes )
    {
        this.isDense = isDense;
        relationshipId = firstRelId;

        this.fromNodeId = fromNodeId;
        this.direction = direction;
        this.relTypes = relTypes;

        if ( isDense && firstRelId != Record.NO_NEXT_RELATIONSHIP.intValue())
        {
            try
            {
                groupRecord = groupStore.getRecord( firstRelId, groupRecordInstance );
                relationshipId = nextChainStart();
            }
            catch ( InvalidRecordException e )
            {
                relationshipId = Record.NO_NEXT_RELATIONSHIP.intValue();
            }

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
        while ( relationshipId != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            relationshipRecord.setId( relationshipId );

            try
            {
                if ( !relationshipStore.fillRecord( relationshipId, relationshipRecord, RecordLoad.CHECK ) )
                {
                    return false;
                }
            }
            catch ( InvalidRecordException e )
            {
                relationshipId = Record.NO_NEXT_RELATIONSHIP.intValue();
                return false;
            }

            try
            {
                // Direction check
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
                    }
                }

                // Type check
                if ( !checkType( relationshipRecord.getType() ) )
                {
                    continue;
                }

                return true;
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
                    throw new InvalidRecordException( "While loading relationships for Node[" + fromNodeId +
                            "] a Relationship[" + relationshipRecord.getId() + "] was encountered that had startNode:" +
                            " " +
                            relationshipRecord.getFirstNode() + " and endNode: " + relationshipRecord.getSecondNode() +
                            ", i.e. which had neither start nor end node as the node we're loading relationships for" );
                }

                // If there are no more relationships, and this is from a dense node, then
                // traverse the next group
                if ( relationshipId == Record.NO_NEXT_RELATIONSHIP.intValue() && isDense )
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
            while ( groupRecord != null )
            {
                if ( checkType( groupRecord.getType() ) )
                {
                    // Go to the next chain (direction) within this group
                    while ( groupChainIndex < GROUP_CHAINS.length )
                    {
                        GroupChain groupChain = GROUP_CHAINS[groupChainIndex++];
                        long chainStart = groupChain.chainStart( groupRecord );
                        if ( chainStart != Record.NO_NEXT_RELATIONSHIP.intValue() &&
                                (direction == Direction.BOTH || groupChain.matchesDirection( direction )) )
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
        }
        catch ( InvalidRecordException e )
        {
            // Ignore - next line will ensure we're finished anyway
        }
        return Record.NO_NEXT_RELATIONSHIP.intValue();
    }

    private boolean checkType( int type )
    {
        if ( relTypes != null )
        {
            for ( int relType : relTypes )
            {
                if ( type == relType )
                {
                    return true;
                }
            }
            return false;

        }
        return true;
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
