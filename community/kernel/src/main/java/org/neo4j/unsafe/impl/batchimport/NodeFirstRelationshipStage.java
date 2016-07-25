/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache;
import org.neo4j.unsafe.impl.batchimport.staging.Stage;

/**
 * Sets {@link NodeRecord#setNextRel(long)} in {@link ParallelBatchImporter}.
 */
public class NodeFirstRelationshipStage extends Stage
{
    public NodeFirstRelationshipStage( String topic, Configuration config, NodeStore nodeStore,
            RecordStore<RelationshipGroupRecord> relationshipGroupStore, NodeRelationshipCache cache,
            boolean denseNodes, int relationshipType )
    {
        super( "Node --> Relationship" + topic, config );
        add( new ReadNodeRecordsByCacheStep( control(), config, nodeStore, cache, denseNodes ) );
        add( new RecordProcessorStep<>( control(), "LINK", config,
                new NodeFirstRelationshipProcessor( relationshipGroupStore, cache, relationshipType ), false ) );
        add( new UpdateRecordsStep<>( control(), config, nodeStore ) );
    }
}
