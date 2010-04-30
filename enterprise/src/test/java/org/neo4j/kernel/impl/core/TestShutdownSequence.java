package org.neo4j.kernel.impl.core;

import static org.junit.Assert.assertEquals;
import static org.neo4j.kernel.impl.AbstractNeo4jTestCase.getStorePath;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.event.ErrorState;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;

@Ignore( "this behavior is under consideration" )
public class TestShutdownSequence
{
    private EmbeddedGraphDatabase graphDb;

    public @Before
    void createGraphDb()
    {
        graphDb = new EmbeddedGraphDatabase( getStorePath( "shutdown" ) );
    }

    public @Test
    void canInvokeShutdownMultipleTimes()
    {
        graphDb.shutdown();
        graphDb.shutdown();
    }

    public @Test
    void eventHandlersAreOnlyInvokedOnceDuringShutdown()
    {
        final AtomicInteger counter = new AtomicInteger();
        graphDb.registerKernelEventHandler( new KernelEventHandler()
        {
            public void beforeShutdown()
            {
                counter.incrementAndGet();
            }

            public Object getResource()
            {
                return null;
            }

            public void kernelPanic( ErrorState error )
            {
            }

            public ExecutionOrder orderComparedTo( KernelEventHandler other )
            {
                return ExecutionOrder.DOESNT_MATTER;
            }
        } );
        graphDb.shutdown();
        graphDb.shutdown();
        assertEquals( 1, counter.get() );
    }

    public @Test
    void canRemoveFilesAndReinvokeShutdown()
    {
        graphDb.shutdown();
        AbstractNeo4jTestCase.deleteFileOrDirectory( new File(
                getStorePath( "shutdown" ) ) );
        graphDb.shutdown();
    }

    public @Test
    void canInvokeShutdownFromShutdownHandler()
    {
        graphDb.registerKernelEventHandler( new KernelEventHandler()
        {
            public void beforeShutdown()
            {
                graphDb.shutdown();
            }

            public Object getResource()
            {
                return null;
            }

            public void kernelPanic( ErrorState error )
            {
            }

            public ExecutionOrder orderComparedTo( KernelEventHandler other )
            {
                return ExecutionOrder.DOESNT_MATTER;
            }
        } );
        graphDb.shutdown();
    }
}
