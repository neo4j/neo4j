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
package visibility;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.concurrent.CountDownLatch;

import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.nioneo.xa.WriteTransaction;
import org.neo4j.test.AbstractSubProcessTestBase;
import org.neo4j.test.subprocess.BreakPoint;
import org.neo4j.test.subprocess.DebugInterface;
import org.neo4j.test.subprocess.DebuggedThread;
import org.neo4j.test.subprocess.KillSubProcess;

@SuppressWarnings( "serial" )
public class TestDatasourceCommitOrderDataVisibility extends AbstractSubProcessTestBase
{
    private final CountDownLatch barrier1 = new CountDownLatch( 1 ), barrier2 = new CountDownLatch( 1 );

    @Test
    public void dataWrittenToTheIndexAndTheGraphShouldNotBeVisibleFromTheIndexBeforeTheGraph() throws Exception
    {
        runInThread( new CreateData() );
        barrier1.await();
        run( new ReadData( true ) );
        barrier2.await();
        run( new ReadData( false ) );
    }

    private static class CreateData implements Task
    {
        @Override
        public void run( EmbeddedGraphDatabase graphdb )
        {
            Node node = graphdb.getReferenceNode();
            Transaction tx = graphdb.beginTx();
            try
            {
                // First do the index operations, to add that data source first
                graphdb.index().forNodes( "nodes" ).add( node, "value", "indexed" );
                // Then update the graph
                node.setProperty( "value", "indexed" );

                enableBreakPoint();

                tx.success();
            }
            finally
            {
                tx.finish();
            }
            done();
        }
    }

    private static class ReadData implements Task
    {
        private final boolean acceptNull;

        ReadData( boolean acceptNull )
        {
            this.acceptNull = acceptNull;
        }

        @Override
        public void run( EmbeddedGraphDatabase graphdb )
        {
            Node node = graphdb.index().forNodes( "nodes" ).get( "value", "indexed" ).getSingle();
            if ( !acceptNull ) assertNotNull( "node not in index", node );
            if ( node != null ) assertEquals( "indexed", node.getProperty( "value", null ) );
            resumeThread();
        }
    }

    static void enableBreakPoint()
    {
        // activates breakpoint...
    }

    static void resumeThread()
    {
        // activates breakpoint...
    }

    static void done()
    {
        // activates breakpoint...
    }

    volatile DebuggedThread thread;

    private final BreakPoint doCommit = new BreakPoint( WriteTransaction.class, "doCommit" )
    {
        @Override
        protected void callback( DebugInterface debug ) throws KillSubProcess
        {
            thread = debug.thread().suspend( this );
            this.disable();
            barrier1.countDown();
        }
    }, enableBreakPoint = new BreakPoint( TestDatasourceCommitOrderDataVisibility.class, "enableBreakPoint" )
    {
        @Override
        protected void callback( DebugInterface debug ) throws KillSubProcess
        {
            doCommit.enable();
        }
    }, resumeThread = new BreakPoint( TestDatasourceCommitOrderDataVisibility.class, "resumeThread" )
    {
        @Override
        protected void callback( DebugInterface debug ) throws KillSubProcess
        {
            thread.resume();
            this.disable();
        }
    }, done = new BreakPoint( TestDatasourceCommitOrderDataVisibility.class, "done" )
    {
        @Override
        protected void callback( DebugInterface debug ) throws KillSubProcess
        {
            this.disable();
            barrier2.countDown();
        }
    };

    @Override
    protected BreakPoint[] breakpoints( int id )
    {
        return new BreakPoint[] { doCommit, enableBreakPoint.enable(), resumeThread.enable(), done.enable() };
    }
}
