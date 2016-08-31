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

import org.neo4j.kernel.impl.store.id.IdGeneratorImpl;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.staging.BatchSender;
import org.neo4j.unsafe.impl.batchimport.staging.Configuration;
import org.neo4j.unsafe.impl.batchimport.staging.ProcessorStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;

/**
 * Assigns record ids to {@link Batch} for later record allocation. Since this step is single-threaded
 * we can safely assign these ids here.
 */
public class AssignRelationshipIdBatchStep extends ProcessorStep<Batch<InputRelationship,RelationshipRecord>>
{
    private long nextId;

    public AssignRelationshipIdBatchStep( StageControl control, Configuration config, long firstRelationshipId )
    {
        super( control, "ASSIGN", config, 1 );
        this.nextId = firstRelationshipId;
    }

    @Override
    protected void process( Batch<InputRelationship,RelationshipRecord> batch, BatchSender sender ) throws Throwable
    {
        if ( nextId <= IdGeneratorImpl.INTEGER_MINUS_ONE &&
                nextId + batch.input.length >= IdGeneratorImpl.INTEGER_MINUS_ONE )
        {
            // There's this pesky INTEGER_MINUS_ONE ID again. Easiest is to simply skip this batch of ids
            // or at least the part up to that id and just continue after it.
            nextId = IdGeneratorImpl.INTEGER_MINUS_ONE + 1;
        }

        // Assign first record id and send
        batch.firstRecordId = nextId;
        sender.send( batch );

        // Set state for the next batch
        nextId += batch.input.length;
    }

    public long getNextRelationshipId()
    {
        return nextId;
    }
}
