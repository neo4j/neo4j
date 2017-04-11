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

import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.staging.BatchSender;
import org.neo4j.unsafe.impl.batchimport.staging.ProcessorStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;

/**
 * Exists only as an adapter in front of {@link PropertyEncoderStep} in {@link BatchInsertRelationshipsStage}.
 */
public class CreateRelationshipRecordsStep extends ProcessorStep<Batch<InputRelationship,RelationshipRecord>>
{
    public CreateRelationshipRecordsStep( StageControl control, Configuration config )
    {
        super( control, "RECORD", config, 0 );
    }

    @Override
    protected void process( Batch<InputRelationship,RelationshipRecord> batch, BatchSender sender ) throws Throwable
    {
        batch.records = new RelationshipRecord[batch.input.length];
        for ( int i = 0; i < batch.records.length; i++ )
        {
            batch.records[i] = new RelationshipRecord( -1 );
        }
        sender.send( batch );
    }
}
