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
package org.neo4j.unsafe.impl.batchimport;

import java.util.function.Predicate;

import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache;
import org.neo4j.unsafe.impl.batchimport.staging.BatchFeedStep;
import org.neo4j.unsafe.impl.batchimport.staging.ReadRecordsStep;
import org.neo4j.unsafe.impl.batchimport.staging.Stage;

import static org.neo4j.unsafe.impl.batchimport.RecordIdIterator.forwards;
import static org.neo4j.unsafe.impl.batchimport.staging.Step.ORDER_SEND_DOWNSTREAM;

public class RelationshipLinkforwardStage extends Stage
{
    public RelationshipLinkforwardStage( String topic, Configuration config, RelationshipStore store,
            NodeRelationshipCache cache, Predicate<RelationshipRecord> readFilter,
            Predicate<RelationshipRecord> denseChangeFilter, int nodeTypes )
    {
        super( "Relationship --> Relationship " + topic, config, ORDER_SEND_DOWNSTREAM );
        add( new BatchFeedStep( control(), config, forwards( 0, store.getHighId(), config ), store.getRecordSize() ) );
        add( new ReadRecordsStep<>( control(), config, true, store, readFilter ) );
        add( new RelationshipLinkforwardStep( control(), config, cache, denseChangeFilter, nodeTypes ) );
        add( new UpdateRecordsStep<>( control(), config, store ) );
    }
}
