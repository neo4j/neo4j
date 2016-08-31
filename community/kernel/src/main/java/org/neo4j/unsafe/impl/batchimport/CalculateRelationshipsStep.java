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
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.staging.BatchSender;
import org.neo4j.unsafe.impl.batchimport.staging.ProcessorStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;

/**
 * Keeps track of number of relationships to import, this to set highId in relationship store before import.
 * This is because of the way double-unit records works, so the secondary units will end up beyond this limit.
 */
public class CalculateRelationshipsStep extends ProcessorStep<Batch<InputRelationship,RelationshipRecord>>
{
    private final RelationshipStore relationshipStore;
    private long numberOfRelationships;

    public CalculateRelationshipsStep( StageControl control, Configuration config, RelationshipStore relationshipStore )
    {
        super( control, "RelationshipCalculator", config, 1 );
        this.relationshipStore = relationshipStore;
    }

    @Override
    protected void process( Batch<InputRelationship,RelationshipRecord> batch, BatchSender sender ) throws Throwable
    {
        numberOfRelationships += batch.input.length;
        sender.send( batch );
    }

    @Override
    protected void done()
    {
        long highestId = relationshipStore.getHighId() + numberOfRelationships;
        relationshipStore.setHighestPossibleIdInUse( highestId );
        super.done();
    }
}
