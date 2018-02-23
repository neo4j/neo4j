/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.staging;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.neo4j.helpers.collection.Pair;
import org.neo4j.unsafe.impl.batchimport.stats.Keys;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.unsafe.impl.batchimport.Configuration.DEFAULT;
import static org.neo4j.unsafe.impl.batchimport.staging.ControlledStep.stepWithAverageOf;
import static org.neo4j.unsafe.impl.batchimport.staging.Step.ORDER_SEND_DOWNSTREAM;

public class StageExecutionTest
{
    @Test
    public void shouldOrderStepsAscending()
    {
        // GIVEN
        Collection<Step<?>> steps = new ArrayList<>();
        steps.add( stepWithAverageOf( "step1", 0, 10 ) );
        steps.add( stepWithAverageOf( "step2", 0, 5 ) );
        steps.add( stepWithAverageOf( "step3", 0, 30 ) );
        StageExecution execution = new StageExecution( "Test", null, DEFAULT, steps, ORDER_SEND_DOWNSTREAM );

        // WHEN
        Iterator<Pair<Step<?>,Float>> ordered = execution.stepsOrderedBy( Keys.avg_processing_time, true ).iterator();

        // THEN
        Pair<Step<?>,Float> fastest = ordered.next();
        assertEquals( fastest.other().floatValue(), 0f, 1f / 2f );
        Pair<Step<?>,Float> faster = ordered.next();
        assertEquals( faster.other().floatValue(), 0f, 1f / 3f );
        Pair<Step<?>,Float> fast = ordered.next();
        assertEquals( fast.other().floatValue(), 0f, 1f );
        assertFalse( ordered.hasNext() );
    }

    @Test
    public void shouldOrderStepsDescending()
    {
        // GIVEN
        Collection<Step<?>> steps = new ArrayList<>();
        steps.add( stepWithAverageOf( "step1", 0, 10 ) );
        steps.add( stepWithAverageOf( "step2", 0, 5 ) );
        steps.add( stepWithAverageOf( "step3", 0, 30 ) );
        steps.add( stepWithAverageOf( "step4", 0, 5 ) );
        StageExecution execution = new StageExecution( "Test", null, DEFAULT, steps, ORDER_SEND_DOWNSTREAM );

        // WHEN
        Iterator<Pair<Step<?>,Float>> ordered = execution.stepsOrderedBy( Keys.avg_processing_time, false ).iterator();

        // THEN
        Pair<Step<?>,Float> slowest = ordered.next();
        assertEquals( slowest.other().floatValue(), 0f, 3f );
        Pair<Step<?>,Float> slower = ordered.next();
        assertEquals( slower.other().floatValue(), 0f, 2f );
        Pair<Step<?>,Float> slow = ordered.next();
        assertEquals( slow.other().floatValue(), 0f, 1f );
        Pair<Step<?>,Float> alsoSlow = ordered.next();
        assertEquals( alsoSlow.other().floatValue(), 0f, 1f );
        assertFalse( ordered.hasNext() );
    }
}
