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
package synchronization;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.lucene.index.IndexWriter;
import org.junit.Test;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.transaction.log.LogRotation;
import org.neo4j.test.AbstractSubProcessTestBase;
import org.neo4j.test.subprocess.BreakPoint;
import org.neo4j.test.subprocess.DebugInterface;
import org.neo4j.test.subprocess.DebuggedThread;
import org.neo4j.test.subprocess.KillSubProcess;

import static org.junit.Assert.assertTrue;

public class TestConcurrentRotation extends AbstractSubProcessTestBase
{
    private final CountDownLatch barrier1 = new CountDownLatch( 1 ), barrier2 = new CountDownLatch( 1 );

    private DebuggedThread thread;

    private final BreakPoint commitIndexWriter = new BreakPoint( IndexWriter.class, "commit" )
    {
        private int counter = 0;

        @Override
        protected void callback( DebugInterface debug ) throws KillSubProcess
        {
            if ( counter++ > 0 )
            {
                return;
            }
            thread = debug.thread().suspend( this );
            this.disable();
            barrier1.countDown();
        }
    };
    private final BreakPoint resumeFlushThread = new BreakPoint( TestConcurrentRotation.class, "resumeFlushThread" )
    {
        @Override
        protected void callback( DebugInterface debug ) throws KillSubProcess
        {
            thread.resume();
            this.disable();
        }
    };
    private final BreakPoint done = new BreakPoint( TestConcurrentRotation.class, "rotateDone" )
    {
        @Override
        protected void callback( DebugInterface debug ) throws KillSubProcess
        {
            this.disable();
            barrier2.countDown();
        }
    };

    static void resumeFlushThread()
    {   // Activates breakpoint
    }

    static void rotateDone()
    {   // Activate breakpoint
    }

    @Override
    protected BreakPoint[] breakpoints( int id )
    {
        return new BreakPoint[] { commitIndexWriter, resumeFlushThread.enable(), done.enable() };
    }

    @Test
    public void rotateLogAtTheSameTimeInitializeIndexWriters() throws Exception
    {
        run( new CreateInitialStateTask() );
        restart();
        commitIndexWriter.enable();
        run( new LoadIndexesTask( 2, false ) );
        RotateIndexLogTask rotateTask = new RotateIndexLogTask();
        runInThread( rotateTask );
        barrier1.await();
        run( new LoadIndexesTask( 3, true ) );
        resumeFlushThread();
        barrier2.await();
        run( new Verifier() );
    }

    private static class Verifier implements Task
    {
        @Override
        public void run( GraphDatabaseAPI graphdb )
        {
            try(Transaction ignored = graphdb.beginTx())
            {
                // TODO: Pass a node reference around of assuming the id will be deterministically assigned,
                // artifact of removing the reference node, upon which this test used to depend.
                assertTrue( (Boolean) graphdb.getNodeById(3).getProperty( "success" ) );
            }
        }
    }

    private static class CreateInitialStateTask implements Task
    {
        @Override
        public void run( GraphDatabaseAPI graphdb )
        {
            try(Transaction tx = graphdb.beginTx())
            {
                for ( int i = 0; i < 3; i++ )
                {
                    graphdb.index().forNodes( "index" + i ).add( graphdb.createNode(), "name", "" + i );
                }
                tx.success();
            }
        }
    }

    private static class LoadIndexesTask implements Task
    {
        private final int count;
        private final boolean resume;

        public LoadIndexesTask( int count, boolean resume )
        {
            this.count = count;
            this.resume = resume;
        }

        @Override
        public void run( GraphDatabaseAPI graphdb )
        {
            try(Transaction ignored = graphdb.beginTx())
            {
                for ( int i = 0; i < count; i++ )
                {
                    graphdb.index().forNodes( "index" + i ).get( "name", i ).getSingle();
                }
            }
            if ( resume )
            {
                resumeFlushThread();
            }
        }
    }

    private static class RotateIndexLogTask implements Task
    {
        @Override
        public void run( GraphDatabaseAPI graphdb )
        {
            try
            {
                rotateLogicalLog( graphdb );
                setSuccess( graphdb, true );
            }
            catch ( Exception e )
            {
                setSuccess( graphdb, false );
                throw Exceptions.launderedException( e );
            }
            finally
            {
                rotateDone();
            }
        }

        private void rotateLogicalLog( GraphDatabaseAPI graphdb ) throws IOException
        {
            graphdb.getDependencyResolver().resolveDependency( LogRotation.class ).rotateLogFile();
        }

        private void setSuccess( GraphDatabaseAPI graphdb, boolean success )
        {
            try(Transaction tx = graphdb.beginTx())
            {
                Node node = graphdb.createNode();
                node.setProperty( "success", success );
                tx.success();
            }
        }
    }
}
