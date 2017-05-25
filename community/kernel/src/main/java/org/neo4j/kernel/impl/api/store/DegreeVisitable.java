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

import org.neo4j.function.Disposable;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.impl.api.DegreeVisitor;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

import static org.neo4j.kernel.impl.api.store.StoreStatement.read;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_RELATIONSHIP;
import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;

public class DegreeVisitable implements DegreeVisitor.Visitable, Disposable
{
    private final RelationshipStore relationshipStore;
    private final RelationshipRecord relationshipRecord;
    private final PageCursor relationshipCursor;
    private final RelationshipGroupStore groupStore;
    private final RelationshipGroupRecord groupRecord;
    private final PageCursor groupCursor;
    private final Consumer<DegreeVisitable> cache;

    private long nodeId;
    private long groupId;

    DegreeVisitable( RelationshipStore relationshipStore, RelationshipGroupStore groupStore,
            Consumer<DegreeVisitable> cache )
    {
        this.relationshipStore = relationshipStore;
        this.relationshipRecord = relationshipStore.newRecord();
        this.relationshipCursor = relationshipStore.newPageCursor();
        this.groupStore = groupStore;
        this.groupRecord = groupStore.newRecord();
        this.groupCursor = groupStore.newPageCursor();
        this.cache = cache;
    }

    public DegreeVisitable init( long nodeId, long firstGroupId )
    {
        this.nodeId = nodeId;
        this.groupId = firstGroupId;
        return this;
    }

    @Override
    public void accept( DegreeVisitor visitor )
    {
        boolean keepGoing = true;
        while ( keepGoing && groupId != NO_NEXT_RELATIONSHIP.longValue() )
        {
            RelationshipGroupRecord record = read( groupId, groupStore, groupRecord, FORCE, groupCursor );
            if ( record.inUse() )
            {
                int type = record.getType();
                long loop = countByFirstPrevPointer( record.getFirstLoop() );
                long outgoing = countByFirstPrevPointer( record.getFirstOut() );
                long incoming = countByFirstPrevPointer( record.getFirstIn() );
                keepGoing = visitor.visitDegree( type, outgoing, incoming, loop );
            }
            groupId = record.getNext();
        }
    }

    private long countByFirstPrevPointer( long relationshipId )
    {
        if ( Record.NO_NEXT_RELATIONSHIP.is( relationshipId ) )
        {
            return 0;
        }
        RelationshipRecord record =
                read( relationshipId, relationshipStore, relationshipRecord, FORCE, relationshipCursor );
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
