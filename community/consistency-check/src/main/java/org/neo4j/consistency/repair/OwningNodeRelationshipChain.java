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
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

public class OwningNodeRelationshipChain
{
    private final RelationshipChainExplorer relationshipChainExplorer;
    private final RecordStore<NodeRecord> nodeStore;
    private final NodeRecord nodeRecord;

    public OwningNodeRelationshipChain( RelationshipChainExplorer relationshipChainExplorer,
                                        RecordStore<NodeRecord> nodeStore )
    {
        this.relationshipChainExplorer = relationshipChainExplorer;
        this.nodeStore = nodeStore;
        this.nodeRecord = nodeStore.newRecord();
    }

    public RecordSet<RelationshipRecord> findRelationshipChainsThatThisRecordShouldBelongTo(
            RelationshipRecord relationship )
    {
        RecordSet<RelationshipRecord> records = new RecordSet<>();
        for ( RelationshipNodeField field : RelationshipNodeField.values() )
        {
            long nodeId = field.get( relationship );
            nodeStore.getRecord( nodeId, nodeRecord, RecordLoad.FORCE );
            records.addAll( relationshipChainExplorer.followChainFromNode( nodeId, nodeRecord.getNextRel() ) );
        }
        return records;
    }
}
