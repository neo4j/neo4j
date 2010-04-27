package org.neo4j.kernel.impl.event;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.event.ErrorState;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class TestKernelEvents
{
    private static final Object RESOURCE1 = new Object();
    private static final Object RESOURCE2 = new Object();
    
    @Test
    public void testRegisterUnregisterHandlers()
    {
        GraphDatabaseService graphDb = new EmbeddedGraphDatabase( "target/var/neodb" );
        KernelEventHandler handler1 = new DummyKernelEventHandler( RESOURCE1 )
        {
            public ExecutionOrder orderComparedTo( KernelEventHandler other )
            {
                return ExecutionOrder.DOESNT_MATTER;
            }
        };
        KernelEventHandler handler2 = new DummyKernelEventHandler( RESOURCE2 )
        {
            public ExecutionOrder orderComparedTo( KernelEventHandler other )
            {
                return ExecutionOrder.DOESNT_MATTER;
            }
        };

        try
        {
            graphDb.unregisterKernelEventHandler( handler1 );
            fail( "Shouldn't be able to do unregister on a "
                  + "unregistered handler" );
        }
        catch ( IllegalStateException e )
        { /* Good */
        }

        assertTrue( handler1 == graphDb.registerKernelEventHandler(
                handler1 ) );
        assertTrue( handler1 == graphDb.registerKernelEventHandler(
                handler1 ) );
        assertTrue( handler1 == graphDb.unregisterKernelEventHandler(
                handler1 ) );

        try
        {
            graphDb.unregisterKernelEventHandler( handler1 );
            fail( "Shouldn't be able to do unregister on a "
                  + "unregistered handler" );
        }
        catch ( IllegalStateException e )
        { /* Good */
        }

        assertTrue( handler1 == graphDb.registerKernelEventHandler(
                handler1 ) );
        assertTrue( handler2 == graphDb.registerKernelEventHandler(
                handler2 ) );
        assertTrue( handler1 == graphDb.unregisterKernelEventHandler(
                handler1 ) );
        assertTrue( handler2 == graphDb.unregisterKernelEventHandler(
                handler2 ) );
        
        graphDb.shutdown();
    }
    
    @Test
    public void testShutdownEvents()
    {
        GraphDatabaseService graphDb = new EmbeddedGraphDatabase( "target/var/neodb" );
        DummyKernelEventHandler handler1 = new DummyKernelEventHandler( RESOURCE1 )
        {
            public ExecutionOrder orderComparedTo( KernelEventHandler other )
            {
                if ( ((DummyKernelEventHandler) other).resource == RESOURCE2 )
                {
                    return ExecutionOrder.AFTER;
                }
                return ExecutionOrder.DOESNT_MATTER;
            }
        };
        DummyKernelEventHandler handler2 = new DummyKernelEventHandler( RESOURCE1 )
        {
            public ExecutionOrder orderComparedTo( KernelEventHandler other )
            {
                if ( ((DummyKernelEventHandler) other).resource == RESOURCE1 )
                {
                    return ExecutionOrder.BEFORE;
                }
                return ExecutionOrder.DOESNT_MATTER;
            }
        };
        graphDb.registerKernelEventHandler( handler1 );
        graphDb.registerKernelEventHandler( handler2 );
        
        graphDb.shutdown();
        
        assertEquals( 0, handler2.beforeShutdown );
        assertEquals( 1, handler1.beforeShutdown );
    }
    
    private static abstract class DummyKernelEventHandler implements KernelEventHandler
    {
        private static int counter;
        private Integer beforeShutdown, kernelPanic;
        private final Object resource;
        
        DummyKernelEventHandler( Object resource )
        {
            this.resource = resource;
        }
        
        public void beforeShutdown()
        {
            beforeShutdown = counter++;
        }

        public Object getResource()
        {
            return this.resource;
        }

        public void kernelPanic( ErrorState error )
        {
            kernelPanic = counter++;
        }
    }
}
