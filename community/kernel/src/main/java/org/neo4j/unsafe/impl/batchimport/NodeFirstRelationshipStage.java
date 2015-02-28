/**
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
package org.neo4j.unsafe.impl.batchimport;

import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipLink;
import org.neo4j.unsafe.impl.batchimport.staging.Stage;

/**
 * Sets {@link NodeRecord#setNextRel(long)} in {@link ParallelBatchImporter}.
 */
public class NodeFirstRelationshipStage extends Stage
{
    public NodeFirstRelationshipStage( Configuration config, NodeStore nodeStore,
            RelationshipGroupStore relationshipGroupStore, NodeRelationshipLink cache )
    {
        super( "Node --> Relationship", config, false );
        add( new ReadNodeRecordsStep( control(), config.batchSize(), config.movingAverageSize(), nodeStore ) );
        add( new NodeFirstRelationshipStep( control(), config.workAheadSize(), config.movingAverageSize(),
                nodeStore, relationshipGroupStore, cache ) );
        add( new UpdateRecordsStep<>( control(), config.workAheadSize(), config.movingAverageSize(), nodeStore ) );
    }
}
