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

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.test.SuppressOutput;

public class LonelyProcessingStepTest
{
    @Rule
    public SuppressOutput mute = SuppressOutput.suppressAll();

    @Test( timeout = 1000 )
    public void issuePanicBeforeCompletionOnError() throws Exception
    {
        List<Step<?>> stepsPipeline = new ArrayList<>();
        FaultyLonelyProcessingStepTest faultyStep = new FaultyLonelyProcessingStepTest( stepsPipeline );
        stepsPipeline.add( faultyStep );

        faultyStep.receive( 1, null );

        while ( !faultyStep.isCompleted() )
        {
            Thread.sleep( 10 );
        }

        Assert.assertTrue( "On upstream end step should be already on panic in case of exception",
                faultyStep.isPanicOnEndUpstream() );
        Assert.assertTrue( faultyStep.isPanic() );
        Assert.assertFalse( faultyStep.stillWorking() );
    }

    private class FaultyLonelyProcessingStepTest extends LonelyProcessingStep
    {

        private volatile boolean panicOnEndUpstream = false;

        public FaultyLonelyProcessingStepTest( List<Step<?>> pipeLine )
        {
            super( new StageExecution( "Faulty", Configuration.DEFAULT, pipeLine, 0 ),
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
            panicOnEndUpstream = isPanic();
            super.endOfUpstream();
        }

        public boolean isPanicOnEndUpstream()
        {
            return panicOnEndUpstream;
        }
    }

}
