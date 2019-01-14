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
package org.neo4j.unsafe.impl.batchimport;

import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache;
import org.neo4j.unsafe.impl.batchimport.cache.NodeType;
import org.neo4j.unsafe.impl.batchimport.staging.ReadRecordsStep;
import org.neo4j.unsafe.impl.batchimport.staging.Stage;
import org.neo4j.unsafe.impl.batchimport.store.StorePrepareIdSequence;

import static org.neo4j.unsafe.impl.batchimport.staging.Step.ORDER_SEND_DOWNSTREAM;
import static org.neo4j.unsafe.impl.batchimport.staging.Step.RECYCLE_BATCHES;

/**
 * Updates sparse {@link NodeRecord node records} with relationship heads after relationship linking. Steps:
 *
 * <ol>
 * <li>{@link ReadNodeIdsByCacheStep} looks at {@link NodeRelationshipCache} for which nodes have had
 * relationships imported and loads those {@link NodeRecord records} from store.</li>
 * <li>{@link RecordProcessorStep} / {@link SparseNodeFirstRelationshipProcessor} uses {@link NodeRelationshipCache}
 * to update each {@link NodeRecord#setNextRel(long)}.
 * <li>{@link UpdateRecordsStep} writes the updated records back into store.</li>
 * </ol>
 */
public class SparseNodeFirstRelationshipStage extends Stage
{
    public static final String NAME = "Node --> Relationship";

    public SparseNodeFirstRelationshipStage( Configuration config, NodeStore nodeStore, NodeRelationshipCache cache )
    {
        super( NAME, null, config, ORDER_SEND_DOWNSTREAM | RECYCLE_BATCHES );
        add( new ReadNodeIdsByCacheStep( control(), config, cache, NodeType.NODE_TYPE_SPARSE ) );
        add( new ReadRecordsStep<>( control(), config, true, nodeStore ) );
        add( new RecordProcessorStep<>( control(), "LINK", config,
                new SparseNodeFirstRelationshipProcessor( cache ), false ) );
        add( new UpdateRecordsStep<>( control(), config, nodeStore, new StorePrepareIdSequence() ) );
    }
}
