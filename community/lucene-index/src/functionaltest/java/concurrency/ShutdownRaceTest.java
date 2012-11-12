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
package concurrency;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.index.impl.lucene.LuceneDataSource;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.transaction.xaframework.XaContainer;
import org.neo4j.test.AbstractSubProcessTestBase;
import org.neo4j.test.subprocess.DebugInterface;
import org.neo4j.test.subprocess.DebuggedThread;
import org.neo4j.test.subprocess.BreakPoint;

public class ShutdownRaceTest extends AbstractSubProcessTestBase
{
    private final CountDownLatch restart = new CountDownLatch( 1 ), last = new CountDownLatch( 1 );
    @Test
    public void canHaveShutdownWhileAccessingIndexWriters() throws Exception
    {
        run( new IndexTask() );
        run( new BreakTask() );
        restart.await();
        restart();
        last.await();
        run( new IndexTask() );
    }

    @Override
    protected BreakPoint[] breakpoints( int id )
    {
        final AtomicReference<DebuggedThread> shutdownThread = new AtomicReference<DebuggedThread>(), indexThread = new AtomicReference<DebuggedThread>();
        return new BreakPoint[] { new BreakPoint( XaContainer.class, "close" )
        {
            @Override
            protected void callback( DebugInterface debug )
            {
                if ( debug.matchCallingMethod( 1, LuceneDataSource.class, null ) )
                {
                    shutdownThread.set( debug.thread().suspend( this ) );
                    resume( indexThread.getAndSet( null ) );
                    this.disable();
                }
            }

            @Override
            public void deadlock( DebuggedThread thread )
            {
                shutdownThread.set( null );
                thread.resume();
            }
        }.enable(), new BreakPoint( BreakTask.class, "breakpoint1" )
        {
            @Override
            protected void callback( DebugInterface debug )
            {
                indexThread.set( debug.thread().suspend( this ) );
                restart.countDown();
            }
        }.enable(), new BreakPoint( BreakTask.class, "breakpoint2" )
        {
            @Override
            protected void callback( DebugInterface debug )
            {
                resume( shutdownThread.getAndSet( null ) );
                last.countDown();
            }
        }.enable() };
    }

    static void resume( DebuggedThread thread )
    {
        if ( thread != null ) thread.resume();
    }

    @SuppressWarnings( "serial" )
    private static class IndexTask implements Task
    {
        @Override
        public void run( final GraphDatabaseAPI graphdb )
        {
            try
            {
                Transaction tx = graphdb.beginTx();
                try
                {
                    index( graphdb.index().forNodes( "name" ), graphdb.getReferenceNode() );
                    tx.success();
                }
                finally
                {
                    tx.finish();
                }
            }
            finally
            {
                done();
            }
        }

        private void index( Index<Node> index, Node node )
        {
            enterIndex();
            index.add( node, getClass().getSimpleName(), Thread.currentThread().getName() );
        }

        protected void enterIndex()
        {
            // override
        }

        protected void done()
        {
            // override
        }
    }

    @SuppressWarnings( "serial" )
    private static class BreakTask extends IndexTask
    {
        @Override
        public void run( final GraphDatabaseAPI graphdb )
        {
            new Thread()
            {
                @Override
                public void run()
                {
                    runTask( graphdb );
                }
            }.start();
        }

        void runTask( GraphDatabaseAPI graphdb )
        {
            super.run( graphdb );
        }

        @Override
        protected void enterIndex()
        {
            breakpoint1();
        }

        @Override
        protected void done()
        {
            breakpoint2();
        }

        private void breakpoint1()
        {
            // the debugger will break here
        }

        private void breakpoint2()
        {
            // the debugger will break here
        }
    }
}
