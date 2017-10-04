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

import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache;
import org.neo4j.unsafe.impl.batchimport.staging.Stage;

/**
 * Takes information about relationship groups in the {@link NodeRelationshipCache}, which is produced
 * as a side-effect of linking relationships together, and writes them out into {@link RelationshipGroupStore}.
 */
public class RelationshipGroupStage extends Stage
{
    public RelationshipGroupStage( String topic, Configuration config,
            RecordStore<RelationshipGroupRecord> store, NodeRelationshipCache cache )
    {
        super( "RelationshipGroup" + topic, config );
        add( new ReadGroupRecordsByCacheStep( control(), config, store, cache ) );
        add( new UpdateRecordsStep<>( control(), config, store ) );
    }
}
