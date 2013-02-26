package org.neo4j.kernel.impl.api.index;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import java.util.concurrent.Future;

import org.junit.Test;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.OtherThreadExecutor.WorkerCommand;

public class BlockingIndexContextTest
{
    @Test
    public void shouldBlockCallsUntilReady() throws Exception
    {
        // GIVEN
        OtherThreadExecutor<Void> otherThread = new OtherThreadExecutor<Void>( "blocking index context", null );
        IndexContext actual = mock( IndexContext.class );
        final BlockingIndexContext context = new BlockingIndexContext( actual );

        // WHEN
        Future<Void> creationFuture = otherThread.executeDontWait( new WorkerCommand<Void, Void>()
        {
            @Override
            public Void doWork( Void state )
            {
                context.create();
                return null;
            }
        } );
        otherThread.waitUntilWaiting();
        verifyZeroInteractions( actual );
        context.ready();
        creationFuture.get();

        // THEN
        verify( actual ).create();
    }
}
