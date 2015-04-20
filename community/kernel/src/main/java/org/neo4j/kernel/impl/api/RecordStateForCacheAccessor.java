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

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntLongMap;
import org.neo4j.kernel.impl.core.FirstRelationshipIds;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.transaction.state.RecordAccessSet;
import org.neo4j.kernel.impl.util.RelIdArray.DirectionWrapper;

/**
 * Has access to record state and makes available some of that information which is needed
 * for updating the cache, for example first relationship ids in each changed relationship chain.
 */
public class RecordStateForCacheAccessor
{
    private final RecordAccessSet access;

    public RecordStateForCacheAccessor( RecordAccessSet access )
    {
        this.access = access;
    }

    public boolean isDense( long nodeId )
    {
        return access.getNodeRecords().getOrLoad( nodeId, null ).forReadingLinkage().isDense();
    }

    public FirstRelationshipIds firstRelationshipIdsOf( long nodeId )
    {
        final NodeRecord node = access.getNodeRecords().getOrLoad( nodeId, null ).forReadingLinkage();
        if ( !node.isDense() )
        {
            // For sparse nodes there's only a single relationship that is going to be the first
            // relationship in the chain, not a relationship per type and direction. So we'll return
            // that single relationship id, knowing that the caller of this method is also aware
            // of the sparse/dense node states and will use this id accordingly.
            return new FirstRelationshipIds()
            {
                @Override
                public long firstIdOf( int type, DirectionWrapper direction )
                {
                    return node.getNextRel();
                }
            };
        }

        final PrimitiveIntLongMap ids = Primitive.intLongMap();
        long groupId = node.getNextRel();
        while ( groupId != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            RelationshipGroupRecord group = access.getRelGroupRecords().getOrLoad( groupId, null ).forReadingData();
            for ( DirectionWrapper direction : DirectionWrapper.values() )
            {
                long firstId = direction.getNextRel( group );
                if ( firstId != Record.NO_NEXT_RELATIONSHIP.intValue() )
                {
                    ids.put( typeKey( group.getType(), direction ), firstId );
                }
            }
            groupId = group.getNext();
        }
        return new FirstRelationshipIds()
        {
            @Override
            public long firstIdOf( int type, DirectionWrapper direction )
            {
                return ids.get( typeKey( type, direction ) );
            }
        };
    }

    private int typeKey( int type, DirectionWrapper direction )
    {
        return type * 3 + direction.ordinal();
    }
}
