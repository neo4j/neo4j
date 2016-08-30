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

import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache;
import org.neo4j.unsafe.impl.batchimport.staging.ReadRecordsStep;
import org.neo4j.unsafe.impl.batchimport.staging.Stage;
import static org.neo4j.unsafe.impl.batchimport.RecordIdIterator.backwards;

/**
 * Sets {@link RelationshipRecord#setFirstPrevRel(long)} and {@link RelationshipRecord#setSecondPrevRel(long)}
 * by going through the {@link RelationshipStore} in reversed order. It uses the {@link NodeRelationshipCache}
 * the same way as {@link RelationshipStage} does to link chains together, but this time for the "prev"
 * pointers of {@link RelationshipRecord}. Steps:
 *
 * <ol>
 * <li>{@link ReadRecordsStep} reads records from store and passes on downwards to be processed.
 * Ids are read page-wise by {@link RecordIdIterator}, where each page is read forwards internally,
 * i.e. the records in the batches are ordered by ascending id and so consecutive steps needs to
 * process the records within each batch from end to start.</li>
 * <li>{@link RelationshipLinkbackStep} processes each batch and assigns the "prev" pointers in
 * {@link RelationshipRecord} by using {@link NodeRelationshipCache}.</li>
 * <li>{@link UpdateRecordsStep} writes the updated records back into store.</li>
 * </ol>
 */
public class RelationshipLinkbackStage extends Stage
{
    public RelationshipLinkbackStage( String topic, Configuration config, RelationshipStore store,
            NodeRelationshipCache cache, long lowRelationshipId, long highRelationshipId, boolean denseNodes )
    {
        super( "Relationship --> Relationship" + topic, config );
        add( new ReadRecordsStep<>( control(), config, store,
                backwards( lowRelationshipId, highRelationshipId, config ) ) );
        add( new RelationshipLinkbackStep( control(), config, cache, denseNodes ) );
        add( new UpdateRecordsStep<>( control(), config, store ) );
    }
}
