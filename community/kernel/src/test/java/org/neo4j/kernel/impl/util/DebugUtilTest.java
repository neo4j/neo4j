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
package org.neo4j.kernel.impl.util;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.concurrent.CountDownLatch;
import org.neo4j.test.OtherThreadExecutor.WorkerCommand;
import org.neo4j.test.OtherThreadRule;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import static org.neo4j.test.DoubleLatch.awaitLatch;

public class DebugUtilTest
{
    private static final String THE_OTHER_THREAD_NAME = "TheOtherThread";

    public final @Rule TestName testName = new TestName();
    @Rule
    public final OtherThreadRule<Void> t2 = new OtherThreadRule<>( THE_OTHER_THREAD_NAME );

    @Test
    public void shouldFigureOutThatThisIsATest()
    {
        assertThat( DebugUtil.trackTest(), containsString( testName.getMethodName() ) );
        assertThat( DebugUtil.trackTest(), containsString( getClass().getSimpleName() ) );
    }

    @Test
    public void shouldFigureOutThatWeStartedInATest() throws Exception
    {
        new Noise().white();
    }

    @Test
    public void shouldDumpThreads() throws Exception
    {
        // GIVEN
        ByteArrayOutputStream capturedOut = new ByteArrayOutputStream();
        PrintStream out = new PrintStream( capturedOut );
        final CountDownLatch latch = new CountDownLatch( 1 );
        t2.execute( new WorkerCommand<Void,Void>()
        {
            @Override
            public Void doWork( Void state ) throws Exception
            {
                awaitLatch( latch );
                return null;
            }
        } );
        t2.get().waitUntilThreadState( Thread.State.WAITING, Thread.State.TIMED_WAITING );

        // WHEN
        DebugUtil.dumpThreads( out );
        latch.countDown();
        out.flush();
        String dump = capturedOut.toString();

        // THEN
        // main
        assertTrue( dump.contains( "main" ) );
        assertTrue( dump.contains( DebugUtilTest.class.getName() ) );
        assertTrue( dump.contains( testName.getMethodName() ) );

        // the other thread
        assertTrue( dump.contains( THE_OTHER_THREAD_NAME ) );
        assertTrue( dump.contains( "doWork" ) );
    }

    private class Noise
    {
        void white()
        {
            assertThat( DebugUtil.trackTest(), containsString( testName.getMethodName() ) );
            assertThat( DebugUtil.trackTest(), containsString( DebugUtilTest.class.getSimpleName() ) );
        }
    }
}
