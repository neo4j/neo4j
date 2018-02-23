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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Resource;

import org.neo4j.concurrent.BinaryLatch;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.unsafe.impl.batchimport.Configuration;

import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith( SuppressOutputExtension.class )
public class LonelyProcessingStepTest
{
    @Resource
    public SuppressOutput mute;

    @Test
    public void issuePanicBeforeCompletionOnError()
    {
        List<Step<?>> stepsPipeline = new ArrayList<>();
        BinaryLatch endOfUpstreamLatch = new BinaryLatch();
        FaultyLonelyProcessingStepTest faultyStep =
                new FaultyLonelyProcessingStepTest( stepsPipeline, endOfUpstreamLatch );
        stepsPipeline.add( faultyStep );

        faultyStep.receive( 1, null );

        endOfUpstreamLatch.await();

        assertTrue( faultyStep.isPanicOnEndUpstream(),
                "On upstream end step should be already on panic in case of exception" );
        Assert.assertTrue( faultyStep.isPanic() );
        Assert.assertFalse( faultyStep.stillWorking() );
        Assert.assertTrue( faultyStep.isCompleted() );
    }

    private class FaultyLonelyProcessingStepTest extends LonelyProcessingStep
    {
        private final BinaryLatch endOfUpstreamLatch;
        private volatile boolean panicOnEndUpstream;

        FaultyLonelyProcessingStepTest( List<Step<?>> pipeLine, BinaryLatch endOfUpstreamLatch )
        {
            super( new StageExecution( "Faulty", null, Configuration.DEFAULT, pipeLine, 0 ),
                    "Faulty", Configuration.DEFAULT );
            this.endOfUpstreamLatch = endOfUpstreamLatch;
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
            endOfUpstreamLatch.release();
        }

        public boolean isPanicOnEndUpstream()
        {
            return panicOnEndUpstream;
        }
    }
}
