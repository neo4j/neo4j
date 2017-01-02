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

import org.junit.Test;

import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;
import org.neo4j.unsafe.impl.batchimport.staging.Step;
import org.neo4j.unsafe.impl.batchimport.stats.Keys;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import static org.neo4j.kernel.impl.store.id.IdGeneratorImpl.INTEGER_MINUS_ONE;
import static org.neo4j.unsafe.impl.batchimport.Configuration.DEFAULT;
import static org.neo4j.unsafe.impl.batchimport.Configuration.withBatchSize;
import static org.neo4j.unsafe.impl.batchimport.staging.Step.ORDER_SEND_DOWNSTREAM;

public class AssignRelationshipIdBatchStepTest
{
    private final StageControl control = mock( StageControl.class );
    private final Configuration config = withBatchSize( DEFAULT, 10 );

    @Test
    public void shouldAssignNewIdsToBatches() throws Exception
    {
        // GIVEN
        try (
                Step<Batch<InputRelationship,RelationshipRecord>> step =
                        new AssignRelationshipIdBatchStep( control, config, 100 );
                CapturingStep<Batch<InputRelationship,RelationshipRecord>> results =
                        new CapturingStep<>( control, "end", config ) )
        {
            step.setDownstream( results );
            step.start( ORDER_SEND_DOWNSTREAM );
            results.start( ORDER_SEND_DOWNSTREAM );

            // WHEN
            Batch<InputRelationship,RelationshipRecord> first = new Batch<>( new InputRelationship[config.batchSize()] );
            step.receive( 0, first );
            Batch<InputRelationship,RelationshipRecord> second = new Batch<>( new InputRelationship[config.batchSize()] );
            step.receive( 1, second );
            while ( results.stats().stat( Keys.done_batches ).asLong() < 2 )
            {
                // wait
            }
            assertEquals( 2, results.receivedBatches().size() );

            // THEN
            assertEquals( 100, first.firstRecordId );
            assertEquals( 100 + config.batchSize(), second.firstRecordId );
        }
    }

    @Test
    public void shouldAvoidReservedId() throws Exception
    {
        // GIVEN
        try ( Step<Batch<InputRelationship,RelationshipRecord>> step =
                      new AssignRelationshipIdBatchStep( control, config, INTEGER_MINUS_ONE - 15 );
              CapturingStep<Batch<InputRelationship,RelationshipRecord>> results = new CapturingStep<>( control, "end", config ) )
        {
            step.setDownstream( results );
            step.start( ORDER_SEND_DOWNSTREAM );
            results.start( ORDER_SEND_DOWNSTREAM );

            // WHEN
            Batch<InputRelationship,RelationshipRecord> first = new Batch<>( new InputRelationship[config.batchSize()] );
            step.receive( 0, first );
            Batch<InputRelationship,RelationshipRecord> second = new Batch<>( new InputRelationship[config.batchSize()] );
            step.receive( 1, second );
            Batch<InputRelationship,RelationshipRecord> third = new Batch<>( new InputRelationship[config.batchSize()] );
            step.receive( 2, third );
            while ( results.stats().stat( Keys.done_batches ).asLong() < 3 )
            {
                // wait
            }
            assertEquals( 3, results.receivedBatches().size() );

            // THEN
            assertEquals( INTEGER_MINUS_ONE - 15, first.firstRecordId );
            assertEquals( INTEGER_MINUS_ONE + 1, second.firstRecordId );
            assertEquals( second.firstRecordId + config.batchSize(), third.firstRecordId );
        }
    }

    @Test
    public void shouldAvoidReservedIdAsFirstAssignment() throws Exception
    {
        // GIVEN
        try (
                Step<Batch<InputRelationship,RelationshipRecord>> step =
                new AssignRelationshipIdBatchStep( control, config, INTEGER_MINUS_ONE - 5 );
                CapturingStep<Batch<InputRelationship,RelationshipRecord>> results =
                        new CapturingStep<>( control, "end", config ) )
        {
            step.setDownstream( results );
            step.start( ORDER_SEND_DOWNSTREAM );
            results.start( ORDER_SEND_DOWNSTREAM );

            // WHEN
            Batch<InputRelationship,RelationshipRecord> first = new Batch<>( new InputRelationship[config.batchSize()] );
            step.receive( 0, first );
            Batch<InputRelationship,RelationshipRecord> second = new Batch<>( new InputRelationship[config.batchSize()] );
            step.receive( 1, second );
            while ( results.stats().stat( Keys.done_batches ).asLong() < 2 )
            {
                // wait
            }
            assertEquals( 2, results.receivedBatches().size() );

            // THEN
            assertEquals( INTEGER_MINUS_ONE + 1, first.firstRecordId );
            assertEquals( first.firstRecordId + config.batchSize(), second.firstRecordId );
        }
    }
}
