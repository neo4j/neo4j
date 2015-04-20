/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package visibility;

import java.util.concurrent.CountDownLatch;

import org.junit.Test;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.test.AbstractSubProcessTestBase;
import org.neo4j.test.subprocess.BreakPoint;
import org.neo4j.test.subprocess.DebugInterface;
import org.neo4j.test.subprocess.DebuggedThread;
import org.neo4j.test.subprocess.KillSubProcess;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@SuppressWarnings( "serial" )
public class TestPropertyReadOnNewEntityBeforeLockRelease extends AbstractSubProcessTestBase
{
    private final CountDownLatch latch1 = new CountDownLatch( 1 ), latch2 = new CountDownLatch( 1 );

    @Test
    public void shouldBeAbleToReadPropertiesFromNewNodeReturnedFromIndex() throws Exception
    {
        runInThread( new CreateData() );
        latch1.await();
        run( new ReadData() );
        latch2.await();
    }

    private static class CreateData implements Task
    {
        @Override
        public void run( GraphDatabaseAPI graphdb )
        {
            try(Transaction tx = graphdb.beginTx())
            {
                Node node = graphdb.createNode();
                node.setProperty( "value", "present" );
                graphdb.index().forNodes( "nodes" ).add( node, "value", "present" );
                enableBreakPoints();

                tx.success();
            }
            done();
        }
    }

    static void enableBreakPoints()
    {
        // triggers breakpoint
    }

    static void done()
    {
        // triggers breakpoint
    }

    static void resumeThread()
    {
        // triggers breakpoint
    }

    private static class ReadData implements Task
    {
        @Override
        public void run( GraphDatabaseAPI graphdb )
        {
            try(Transaction ignored = graphdb.beginTx())
            {
                Node node = graphdb.index().forNodes( "nodes" ).get( "value", "present" ).getSingle();
                assertNotNull( "did not get the node from the index", node );
                assertEquals( "present", node.getProperty( "value" ) );
            }
            resumeThread();
        }
    }

    private volatile DebuggedThread thread;
    private final BreakPoint lockReleaserCommit = new BreakPoint( KernelTransactionImplementation.class, "release" )
    {
        @Override
        protected void callback( DebugInterface debug ) throws KillSubProcess
        {
            thread = debug.thread().suspend( this );
            resumeThread.enable();
            this.disable();
            latch1.countDown();
        }
    }, enableBreakPoints = new BreakPoint( TestPropertyReadOnNewEntityBeforeLockRelease.class, "enableBreakPoints" )
    {
        @Override
        protected void callback( DebugInterface debug ) throws KillSubProcess
        {
            lockReleaserCommit.enable();
            this.disable();
        }
    }, resumeThread = new BreakPoint( TestPropertyReadOnNewEntityBeforeLockRelease.class, "resumeThread" )
    {
        @Override
        protected void callback( DebugInterface debug ) throws KillSubProcess
        {
            thread.resume();
            this.disable();
        }
    }, done = new BreakPoint( TestPropertyReadOnNewEntityBeforeLockRelease.class, "done" )
    {
        @Override
        protected void callback( DebugInterface debug ) throws KillSubProcess
        {
            latch2.countDown();
            this.disable();
        }
    };

    @Override
    protected BreakPoint[] breakpoints( int id )
    {
        return new BreakPoint[] { lockReleaserCommit, enableBreakPoints.enable(), resumeThread, done.enable() };
    }

    /**
     * Version of the test case useful for manual debugging.
     */
    public static void main( String... args ) throws Exception
    {
        final GraphDatabaseAPI graphdb = (GraphDatabaseAPI) new GraphDatabaseFactory().
                                                  newEmbeddedDatabase( "target/test-data/" + TestPropertyReadOnNewEntityBeforeLockRelease.class
                                                      .getName() + "/graphdb" );
        final CountDownLatch completion = new CountDownLatch( 2 );
        class TaskRunner implements Runnable
        {
            private final Task task;

            TaskRunner( Task task )
            {
                this.task = task;
            }

            @Override
            public void run()
            {
                try
                {
                    task.run( graphdb );
                }
                finally
                {
                    completion.countDown();
                }
            }
        }
        new Thread( new TaskRunner( new CreateData() ) ).start();
        new Thread( new TaskRunner( new ReadData() ) ).start();
        try
        {
            completion.await();
        }
        finally
        {
            graphdb.shutdown();
        }
    }
}
