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
package org.neo4j.unsafe.impl.batchimport.staging;

import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.unsafe.impl.batchimport.Configuration;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LonelyProcessingStepTest
{
    @ClassRule
    public static SuppressOutput mute = SuppressOutput.suppressAll();

    @Test( timeout = 10_000 )
    public void issuePanicBeforeCompletionOnError() throws InterruptedException
    {
        List<Step<?>> stepsPipeline = new ArrayList<>();
        FaultyLonelyProcessingStepTest faultyStep = new FaultyLonelyProcessingStepTest( stepsPipeline );
        stepsPipeline.add( faultyStep );

        faultyStep.receive( 1, null );
        faultyStep.awaitCompleted();

        assertTrue( faultyStep.endOfUpstreamCalled );
        assertTrue( "On upstream end step should be already on panic in case of exception",
                faultyStep.isPanicOnEndUpstream() );
        assertTrue( faultyStep.isPanic() );
        assertFalse( faultyStep.stillWorking() );
        assertTrue( faultyStep.isCompleted() );
    }

    private class FaultyLonelyProcessingStepTest extends LonelyProcessingStep
    {
        private volatile boolean endOfUpstreamCalled;
        private volatile boolean panicOnEndUpstream;

        FaultyLonelyProcessingStepTest( List<Step<?>> pipeLine )
        {
            super( new StageExecution( "Faulty", null, Configuration.DEFAULT, pipeLine, 0 ),
                    "Faulty", Configuration.DEFAULT );
        }

        @Override
        protected void process()
        {
            throw new RuntimeException( "Process exception" );
        }

        @Override
        public void endOfUpstream()
        {
            endOfUpstreamCalled = true;
            panicOnEndUpstream = isPanic();
            super.endOfUpstream();
        }

        private boolean isPanicOnEndUpstream()
        {
            return panicOnEndUpstream;
        }
    }
}
