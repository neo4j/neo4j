/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.metatest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.model.MultipleFailureException;
import org.neo4j.test.subprocess.BreakPoint;
import org.neo4j.test.subprocess.BreakPoint.Event;
import org.neo4j.test.subprocess.BreakpointHandler;
import org.neo4j.test.subprocess.BreakpointTrigger;
import org.neo4j.test.subprocess.DebugInterface;
import org.neo4j.test.subprocess.DebuggedThread;
import org.neo4j.test.subprocess.EnabledBreakpoints;
import org.neo4j.test.subprocess.SubProcess;
import org.neo4j.test.subprocess.SubProcessTestRunner;
import org.neo4j.test.subprocess.SuspendedThreadsException;
import org.neo4j.test.subprocess.Task;

@Ignore( "not stable enough" )
@RunWith( SubProcessTestRunner.class )
public class TestSubProcessTestRunner
{
    @Test
    public void testsRunInASubprocess()
    {
        StackTraceElement[] trace = Thread.currentThread().getStackTrace();
        assertEquals( SubProcess.class.getName(), trace[trace.length - 1].getClassName() );
    }

    @EnabledBreakpoints( { "sleep forever", "wait for sleep" } )
    @Test( expected = SuspendedThreadsException.class )
    public void breakpointsNeedToReleaseBeforeTheTestIsDone()
    {
        new Thread()
        {
            @Override
            public void run()
            {
                triggerBreakpointThatNeverWakesUp();
            }
        }.start();
        triggerBreakpointThatWaitsForOtherBreakpoint();
    }

    @BreakpointTrigger( "wait for sleep" )
    private void triggerBreakpointThatWaitsForOtherBreakpoint()
    {
        // do nothing but trigger the breakpoint
    }

    private static DebuggedThread waiter;

    @BreakpointHandler( "wait for sleep" )
    public static void handleWait( DebugInterface di )
    {
        waiter = waiter != null ? null : di.thread().suspend( null );
    }

    @BreakpointTrigger( "sleep forever" )
    private void triggerBreakpointThatNeverWakesUp()
    {
        // do nothing but trigger the breakpoint
    }

    @BreakpointHandler( "sleep forever" )
    public static void sleepForever( DebugInterface di )
    {
        if ( waiter != null )
        {
            waiter.resume();
            waiter = null;
        }
        else
        {
            waiter = di.thread().suspend( null );
        }
    }

    private volatile String[] nulls = null;

    @Ignore( "does not work due to the threading policy of RMI" )
    @EnabledBreakpoints( "testsCanBeBreakpoints" )
    @BreakpointTrigger
    @Test
    public void testsCanBeBreakpoints() throws Throwable
    {
        assertNotNull( "task execution failed to communicate message", nulls );
        List<Throwable> failures = new ArrayList<Throwable>();
        for ( String id : nulls )
        {
            try
            {
                assertNotNull( id, null );
            }
            catch ( Throwable e )
            {
                failures.add( e );
            }
        }
        MultipleFailureException.assertEmpty( failures );
    }
    
    private static DebuggedThread suspended;

    @BreakpointHandler( "testsCanBeBreakpoints" )
    public static void validateInjections( DebugInterface di, BreakPoint self,
            @BreakpointHandler( "resume" ) BreakPoint resume, Task.Executor executor )
    {
        suspended = di.thread().suspend( null );
        resume.enable();
        
        Map<String, Object> injectable = new HashMap<String, Object>();
        injectable.put( "self breakpoint", self );
        injectable.put( DebugInterface.class.getSimpleName(), di );
        injectable.put( "referenced breakpoint", resume );
        injectable.put( "task executor", executor );
        final String[] nulls = validateNotNull( injectable );
        executor.submit( new Task<TestSubProcessTestRunner>()
        {
            @Override
            public void run( TestSubProcessTestRunner test )
            {
                test.resume( nulls );
            }
        } );
    }

    @BreakpointTrigger( on = Event.EXIT )
    private void resume( String[] nulls )
    {
        this.nulls = nulls;
    }

    @BreakpointHandler( "resume" )
    public static void handleResume()
    {
        suspended.resume();
        suspended = null;
    }

    private static String[] validateNotNull( Map<String, Object> injectable )
    {
        List<String> nulls = new ArrayList<String>();
        for ( Map.Entry<String, Object> entry : injectable.entrySet() )
        {

        }
        return nulls.toArray( new String[nulls.size()] );
    }
}
