/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.util.function.Consumer;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.impl.api.DegreeVisitor;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.storageengine.api.Direction;

import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_RELATIONSHIP;
import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;

public class DenseNodeDegreeCounter implements NodeDegreeCounter
{
    private final RelationshipStore relationshipStore;
    private final RelationshipRecord relationshipRecord;
    private final PageCursor relationshipCursor;
    private final RelationshipGroupStore groupStore;
    private final RelationshipGroupRecord groupRecord;
    private final PageCursor groupCursor;
    private final Consumer<DenseNodeDegreeCounter> cache;

    private long nodeId;
    private long groupId;

    DenseNodeDegreeCounter( RelationshipStore relationshipStore, RelationshipGroupStore groupStore,
            Consumer<DenseNodeDegreeCounter> cache )
    {
        this.relationshipStore = relationshipStore;
        this.relationshipRecord = relationshipStore.newRecord();
        this.relationshipCursor = relationshipStore.newPageCursor();
        this.groupStore = groupStore;
        this.groupRecord = groupStore.newRecord();
        this.groupCursor = groupStore.newPageCursor();
        this.cache = cache;
    }

    public DenseNodeDegreeCounter init( long nodeId, long firstGroupId )
    {
        this.nodeId = nodeId;
        this.groupId = firstGroupId;
        return this;
    }

    @Override
    public void accept( DegreeVisitor visitor )
    {
        while ( !NO_NEXT_RELATIONSHIP.is( groupId ) )
        {
            RelationshipGroupRecord record = groupStore.readRecord( groupId, groupRecord, FORCE, groupCursor );
            if ( record.inUse() )
            {
                int type = record.getType();
                long loop = countByFirstPrevPointer( record.getFirstLoop() );
                long outgoing = countByFirstPrevPointer( record.getFirstOut() ) + loop;
                long incoming = countByFirstPrevPointer( record.getFirstIn() ) + loop;
                visitor.visitDegree( type, outgoing, incoming );
            }
            groupId = record.getNext();
        }
    }

    @Override
    public int count( Direction direction )
    {
        return countRelationshipsInGroup( direction, -1 );
    }

    public int count( Direction direction, int relType )
    {
        return countRelationshipsInGroup( direction, relType );
    }

    private long countByFirstPrevPointer( long relationshipId )
    {
        if ( NO_NEXT_RELATIONSHIP.is( relationshipId ) )
        {
            return 0;
        }
        RelationshipRecord record =
                relationshipStore.readRecord( relationshipId, relationshipRecord, FORCE, relationshipCursor );
        if ( record.getFirstNode() == nodeId )
        {
            return record.getFirstPrevRel();
        }
        if ( record.getSecondNode() == nodeId )
        {
            return record.getSecondPrevRel();
        }
        throw new InvalidRecordException( "Node " + nodeId + " neither start nor end node of " + record );
    }

    private int countRelationshipsInGroup( Direction direction, int type )
    {
        int count = 0;
        while ( !NO_NEXT_RELATIONSHIP.is( groupId ) )
        {
            RelationshipGroupRecord record = groupStore.readRecord( groupId, groupRecord, FORCE, groupCursor );
            if ( record.inUse() && ( type == -1 || groupRecord.getType() == type ) )
            {
                count += nodeDegreeByDirection( direction );
                if ( type != -1 )
                {
                    // we have read the only type we were interested on, so break the look
                    break;
                }
            }
            groupId = groupRecord.getNext();
        }
        return count;
    }

    private long nodeDegreeByDirection( Direction direction )
    {
        long firstLoop = groupRecord.getFirstLoop();
        long loopCount = countByFirstPrevPointer( firstLoop );
        switch ( direction )
        {
        case OUTGOING:
        {
            long firstOut = groupRecord.getFirstOut();
            return countByFirstPrevPointer( firstOut ) + loopCount;
        }
        case INCOMING:
        {
            long firstIn = groupRecord.getFirstIn();
            return countByFirstPrevPointer( firstIn ) + loopCount;
        }
        case BOTH:
        {
            long firstOut = groupRecord.getFirstOut();
            long firstIn = groupRecord.getFirstIn();
            return countByFirstPrevPointer( firstOut ) +
                    countByFirstPrevPointer( firstIn ) + loopCount;
        }
        default:
            throw new IllegalArgumentException( direction.name() );
        }
    }

    @Override
    public void close()
    {
        nodeId = StatementConstants.NO_SUCH_NODE;
        groupId = StatementConstants.NO_SUCH_RELATIONSHIP;
        cache.accept( this );
    }

    @Override
    public void dispose()
    {
        groupCursor.close();
        relationshipCursor.close();
    }
}
