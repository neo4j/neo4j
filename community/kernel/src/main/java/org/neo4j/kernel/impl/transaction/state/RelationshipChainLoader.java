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
package org.neo4j.kernel.impl.transaction.state;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.helpers.Pair;
import org.neo4j.kernel.impl.api.DegreeVisitor;
import org.neo4j.kernel.impl.core.DenseNodeChainPosition;
import org.neo4j.kernel.impl.core.RelationshipLoadingPosition;
import org.neo4j.kernel.impl.core.SingleChainPosition;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.util.RelIdArray.DirectionWrapper;

/**
 * Able to load relationship chains, relationship types and degrees from relationship records in a {@link NeoStore}.
 */
public class RelationshipChainLoader
{
    private final NodeStore nodeStore;
    private final RelationshipStore relationshipStore;
    private final RelationshipGroupStore relationshipGroupStore;
    private final int relationshipGrabSize;

    public RelationshipChainLoader( NeoStore neoStore )
    {
        this.nodeStore = neoStore.getNodeStore();
        this.relationshipStore = neoStore.getRelationshipStore();
        this.relationshipGroupStore = neoStore.getRelationshipGroupStore();
        this.relationshipGrabSize = neoStore.getRelationshipGrabSize();
    }

    public static int relCount( long nodeId, RelationshipRecord rel )
    {
        return (int) (nodeId == rel.getFirstNode() ? rel.getFirstPrevRel() : rel.getSecondPrevRel());
    }

    public Pair<Map<DirectionWrapper, Iterable<RelationshipRecord>>, RelationshipLoadingPosition>
            getMoreRelationships( long nodeId, RelationshipLoadingPosition originalPosition,
                    DirectionWrapper direction, int[] types )
    {
        // initialCapacity=grabSize saves the lists the trouble of resizing
        List<RelationshipRecord> out = new ArrayList<>();
        List<RelationshipRecord> in = new ArrayList<>();
        List<RelationshipRecord> loop = null;
        Map<DirectionWrapper, Iterable<RelationshipRecord>> result = new EnumMap<>( DirectionWrapper.class );
        result.put( DirectionWrapper.OUTGOING, out );
        result.put( DirectionWrapper.INCOMING, in );
        RelationshipLoadingPosition loadPosition = originalPosition.clone();
        long position = loadPosition.position( direction, types );
        RelationshipRecord relRecord = null;
        boolean allocateNewRecord = true;
        for ( int i = 0; i < relationshipGrabSize && position != Record.NO_NEXT_RELATIONSHIP.intValue(); i++ )
        {
            if ( allocateNewRecord )
            {
                relRecord = new RelationshipRecord( -1 );
                allocateNewRecord = false;
            }

            if ( !relationshipStore.fillChainRecord( position, relRecord ) )
            {
                // return what we got so far
                return Pair.of( result, loadPosition );
            }
            long firstNode = relRecord.getFirstNode();
            long secondNode = relRecord.getSecondNode();
            if ( relRecord.inUse() )
            {
                if ( firstNode == secondNode )
                {
                    if ( loop == null )
                    {
                        // This is done lazily because loops are probably quite
                        // rarely encountered
                        loop = new ArrayList<>();
                        result.put( DirectionWrapper.BOTH, loop );
                    }
                    loop.add( relRecord );
                    allocateNewRecord = true;
                }
                else if ( firstNode == nodeId )
                {
                    out.add( relRecord );
                    allocateNewRecord = true;
                }
                else if ( secondNode == nodeId )
                {
                    in.add( relRecord );
                    allocateNewRecord = true;
                }
            }
            else
            {
                i--;
            }
            long next = followRelationshipChain( nodeId, relRecord );
            position = loadPosition.nextPosition( next, direction, types );
        }
        return Pair.of( result, loadPosition );
    }

    public static long followRelationshipChain( long nodeId, RelationshipRecord relRecord )
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

    public int getRelationshipCount( long id, int type, DirectionWrapper direction )
    {
        NodeRecord node = nodeStore.getRecord( id );
        long nextRel = node.getNextRel();
        if ( nextRel == Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            return 0;
        }
        if ( !node.isDense() )
        {
            assert type == -1;
            assert direction == DirectionWrapper.BOTH;
            return getRelationshipCount( node, nextRel );
        }

        // From here on it's only dense node specific

        Map<Integer, RelationshipGroupRecord> groups = loadRelationshipGroups( node );
        if ( type == -1 && direction == DirectionWrapper.BOTH )
        {   // Count for all types/directions
            int count = 0;
            for ( RelationshipGroupRecord group : groups.values() )
            {
                count += getRelationshipCount( node, group.getFirstOut() );
                count += getRelationshipCount( node, group.getFirstIn() );
                count += getRelationshipCount( node, group.getFirstLoop() );
            }
            return count;
        }
        else if ( type == -1 )
        {   // Count for all types with a given direction
            int count = 0;
            for ( RelationshipGroupRecord group : groups.values() )
            {
                count += getRelationshipCount( node, group, direction );
            }
            return count;
        }
        else if ( direction == DirectionWrapper.BOTH )
        {   // Count for a type
            RelationshipGroupRecord group = groups.get( type );
            if ( group == null )
            {
                return 0;
            }
            int count = 0;
            count += getRelationshipCount( node, group.getFirstOut() );
            count += getRelationshipCount( node, group.getFirstIn() );
            count += getRelationshipCount( node, group.getFirstLoop() );
            return count;
        }
        else
        {   // Count for one type and direction
            RelationshipGroupRecord group = groups.get( type );
            if ( group == null )
            {
                return 0;
            }
            return getRelationshipCount( node, group, direction );
        }
    }

    public void visitRelationshipCounts( long nodeId, DegreeVisitor visitor )
    {
        NodeRecord node = nodeStore.getRecord( nodeId );
        long nextRecord = node.getNextRel();
        if ( Record.NO_NEXT_RELATIONSHIP.is( nextRecord ) )
        {
            return;
        }
        if ( !node.isDense() )
        {
            throw new UnsupportedOperationException( "non-dense nodes should be handled by the cache layer" );
        }
        // visit the counts of this dense node
        while ( !Record.NO_NEXT_RELATIONSHIP.is( nextRecord ) )
        {
            RelationshipGroupRecord group = relationshipGroupStore.getRecord( nextRecord );
            nextRecord = group.getNext();
            int outgoing = getRelationshipCount( node, group.getFirstOut() );
            int incoming = getRelationshipCount( node, group.getFirstIn() );
            int loops = getRelationshipCount( node, group.getFirstLoop() );
            visitor.visitDegree( group.getType(), outgoing + loops, incoming + loops );
        }
    }

    private int getRelationshipCount( NodeRecord node, RelationshipGroupRecord group, DirectionWrapper direction )
    {
        if ( direction == DirectionWrapper.BOTH )
        {
            return getRelationshipCount( node, DirectionWrapper.OUTGOING.getNextRel( group ) ) +
                    getRelationshipCount( node, DirectionWrapper.INCOMING.getNextRel( group ) ) +
                    getRelationshipCount( node, DirectionWrapper.BOTH.getNextRel( group ) );
        }

        return getRelationshipCount( node, direction.getNextRel( group ) ) +
                getRelationshipCount( node, DirectionWrapper.BOTH.getNextRel( group ) );
    }

    private int getRelationshipCount( NodeRecord node, long relId )
    {   // Relationship count is in a PREV field of the first record in a chain
        if ( relId == Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            return 0;
        }
        RelationshipRecord rel = relationshipStore.getRecord( relId );
        return (int) (node.getId() == rel.getFirstNode() ? rel.getFirstPrevRel() : rel.getSecondPrevRel());
    }

    public Integer[] getRelationshipTypes( long id )
    {
        Map<Integer, RelationshipGroupRecord> groups = loadRelationshipGroups( nodeStore.getRecord( id ) );
        Integer[] types = new Integer[groups.size()];
        int i = 0;
        for ( Integer type : groups.keySet() )
        {
            types[i++] = type;
        }
        return types;
    }

    public RelationshipLoadingPosition getRelationshipChainPosition( long id )
    {
        NodeRecord node;
        try
        {
            node = nodeStore.getRecord( id );
        }
        catch ( InvalidRecordException e )
        {
            return RelationshipLoadingPosition.EMPTY;
        }

        if ( node.isDense() )
        {
            long firstGroup = node.getNextRel();
            if ( firstGroup == Record.NO_NEXT_RELATIONSHIP.intValue() )
            {
                return RelationshipLoadingPosition.EMPTY;
            }
            Map<Integer, RelationshipGroupRecord> groups = loadRelationshipGroups( node );
            return new DenseNodeChainPosition( groups );
        }

        long firstRel = node.getNextRel();
        return firstRel == Record.NO_NEXT_RELATIONSHIP.intValue() ?
                RelationshipLoadingPosition.EMPTY : new SingleChainPosition( firstRel );
    }

    private Map<Integer, RelationshipGroupRecord> loadRelationshipGroups( NodeRecord node )
    {
        assert node.isDense();
        long groupId = node.getNextRel();
        long previousGroupId = Record.NO_NEXT_RELATIONSHIP.intValue();
        Map<Integer, RelationshipGroupRecord> result = new HashMap<>();
        while ( groupId != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            RelationshipGroupRecord record = relationshipGroupStore.getRecord( groupId );
            record.setPrev( previousGroupId );
            result.put( record.getType(), record );
            previousGroupId = groupId;
            groupId = record.getNext();
        }
        return result;
    }
}
