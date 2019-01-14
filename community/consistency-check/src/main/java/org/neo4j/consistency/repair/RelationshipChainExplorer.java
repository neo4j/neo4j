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
package org.neo4j.consistency.repair;

import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

import static org.neo4j.consistency.repair.RelationshipChainDirection.NEXT;
import static org.neo4j.consistency.repair.RelationshipChainDirection.PREV;
import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

public class RelationshipChainExplorer
{
    private final RecordStore<RelationshipRecord> recordStore;

    public RelationshipChainExplorer( RecordStore<RelationshipRecord> recordStore )
    {
        this.recordStore = recordStore;
    }

    public RecordSet<RelationshipRecord> exploreRelationshipRecordChainsToDepthTwo( RelationshipRecord record )
    {
        RecordSet<RelationshipRecord> records = new RecordSet<>();
        for ( RelationshipNodeField nodeField : RelationshipNodeField.values() )
        {
            long nodeId = nodeField.get( record );
            records.addAll( expandChains( expandChainInBothDirections( record, nodeId ), nodeId ) );
        }
        return records;
    }

    private RecordSet<RelationshipRecord> expandChains( RecordSet<RelationshipRecord> records, long otherNodeId )
    {
        RecordSet<RelationshipRecord> chains = new RecordSet<>();
        for ( RelationshipRecord record : records )
        {
            chains.addAll( expandChainInBothDirections( record,
                    record.getFirstNode() == otherNodeId ? record.getSecondNode() : record.getFirstNode() ) );
        }
        return chains;
    }

    private RecordSet<RelationshipRecord> expandChainInBothDirections( RelationshipRecord record, long nodeId )
    {
        return expandChain( record, nodeId, PREV ).union( expandChain( record, nodeId, NEXT ) );
    }

    protected RecordSet<RelationshipRecord> followChainFromNode( long nodeId, long relationshipId )
    {
        return expandChain( recordStore.getRecord( relationshipId, recordStore.newRecord(), NORMAL ), nodeId, NEXT );
    }

    private RecordSet<RelationshipRecord> expandChain( RelationshipRecord record, long nodeId,
                                                       RelationshipChainDirection direction )
    {
        RecordSet<RelationshipRecord> chain = new RecordSet<>();
        chain.add( record );
        RelationshipRecord currentRecord = record;
        long nextRelId = direction.fieldFor( nodeId, currentRecord ).relOf( currentRecord );
        while ( currentRecord.inUse() && !direction.fieldFor( nodeId, currentRecord ).endOfChain( currentRecord ) )
        {
            currentRecord = recordStore.getRecord( nextRelId, recordStore.newRecord(), FORCE );
            chain.add( currentRecord );
            nextRelId = direction.fieldFor( nodeId, currentRecord ).relOf( currentRecord );
        }
        return chain;
    }
}
