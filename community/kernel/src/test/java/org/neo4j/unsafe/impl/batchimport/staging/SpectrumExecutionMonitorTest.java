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
package org.neo4j.unsafe.impl.batchimport.staging;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import org.neo4j.unsafe.impl.batchimport.stats.Keys;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class SpectrumExecutionMonitorTest
{
    @Test
    public void shouldAlternateStagesWithMultiple() throws Exception
    {
        // GIVEN
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        PrintStream out = new PrintStream( bytes );
        SpectrumExecutionMonitor monitor = new SpectrumExecutionMonitor( 0, MILLISECONDS, new PrintStream( out ), 100 );
        StageExecution[] stages = new StageExecution[] {
                stage( "A_1", "A_2" ),
                stage( "B_1", "B_2" ),
        };

        {
            // WHEN
            monitor.check( stages );
            String[] lines = linesOf( out, bytes );

            // THEN
            assertEquals( 1, lines.length );
            assertTrue( lines[0], lines[0].contains( "A_1" ) );
            assertFalse( lines[0], lines[0].contains( "B_1" ) );
        }

        {
            // and WHEN
            monitor.check( stages );
            String[] lines = linesOf( out, bytes );

            // THEN
            assertEquals( 2, lines.length );
            assertTrue( lines[1], lines[1].contains( "B_1" ) );
            assertFalse( lines[1], lines[1].contains( "A_1" ) );
        }

        {
            // and WHEN
            monitor.check( stages );
            String[] lines = linesOf( out, bytes );

            // THEN
            assertEquals( 3, lines.length );
            assertTrue( lines[2], lines[0].contains( "A_1" ) );
            assertFalse( lines[2], lines[0].contains( "B_1" ) );
        }
    }

    @Test
    public void shouldOnlyAlternativeBetweenActiveStages() throws Exception
    {
        // GIVEN
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        PrintStream out = new PrintStream( bytes );
        SpectrumExecutionMonitor monitor = new SpectrumExecutionMonitor( 0, MILLISECONDS, new PrintStream( out ), 100 );
        StageExecution stageB;
        StageExecution[] stages = new StageExecution[] {
                stage( "A_1", "A_2" ),
                stageB = stage( "B_1", "B_2" ),
                stage( "C_1", "C_2" )
        };

        {
            // WHEN
            monitor.check( stages );
            String[] lines = linesOf( out, bytes );

            // THEN
            assertEquals( 1, lines.length );
            assertTrue( lines[0], lines[0].contains( "A_1" ) );
            assertFalse( lines[0], lines[0].contains( "B_1" ) );
        }

        complete( stageB );

        {
            // WHEN
            monitor.check( stages );
            String[] lines = linesOf( out, bytes );

            // THEN
            assertEquals( 2, lines.length );
            assertTrue( lines[1], lines[1].contains( "C_1" ) );
            assertFalse( lines[1], lines[1].contains( "A_1" ) );
            assertFalse( lines[1], lines[1].contains( "B_1" ) );
        }

        {
            // WHEN
            monitor.check( stages );
            String[] lines = linesOf( out, bytes );

            // THEN
            assertEquals( 3, lines.length );
            assertTrue( lines[2], lines[2].contains( "A_1" ) );
            assertFalse( lines[2], lines[2].contains( "B_1" ) );
            assertFalse( lines[2], lines[2].contains( "C_1" ) );
        }
    }

    private void complete( StageExecution stage )
    {
        for ( Step<?> step : stage.steps() )
        {
            ((ControlledStep<?>) step).complete();
        }
    }

    private String[] linesOf( PrintStream out, ByteArrayOutputStream bytes )
    {
        out.flush();
        String string = bytes.toString();
        String[] allLines = string.split( "\r" );
        return Arrays.copyOfRange( allLines, 1, allLines.length );
    }

    private StageExecution stage( String... stepNames )
    {
        Collection<Step<?>> pipeline = new ArrayList<>();
        long avg = 10;
        for ( String name : stepNames )
        {
            pipeline.add( ControlledStep.stepWithStats( name, 1, Keys.avg_processing_time, avg, Keys.done_batches, 1L ) );
            avg += 10;
        }
        return new StageExecution( "Test", Configuration.DEFAULT, pipeline, 0 );
    }
}
