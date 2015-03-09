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

import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipLink;
import org.neo4j.unsafe.impl.batchimport.staging.BatchSender;
import org.neo4j.unsafe.impl.batchimport.staging.ProcessorStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;

/**
 * Sets {@link NodeRecord#setNextRel(long)} in {@link ParallelBatchImporter}.
 */
public class NodeFirstRelationshipStep extends ProcessorStep<NodeRecord[]>
{
    private final NodeFirstRelationshipProcessor processor;

    public NodeFirstRelationshipStep( StageControl control, Configuration config,
            RelationshipGroupStore relationshipGroupStore, NodeRelationshipLink cache )
    {
        super( control, "Node --> Relationship", config );
        this.processor = new NodeFirstRelationshipProcessor( relationshipGroupStore, cache );
    }

    @Override
    protected void process( NodeRecord[] batch, BatchSender sender )
    {
        for ( NodeRecord node : batch )
        {
            if ( node != null )
            {
                processor.process( node );
            }
        }
        sender.send( batch );
    }
}
