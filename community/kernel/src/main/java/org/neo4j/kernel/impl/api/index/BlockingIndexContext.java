package org.neo4j.kernel.impl.api.index;

import java.util.concurrent.CountDownLatch;

public class BlockingIndexContext extends DelegatingIndexContext
{
    private final CountDownLatch latch = new CountDownLatch( 1 );
    
    public BlockingIndexContext( IndexContext delegate )
    {
        super( delegate );
    }

    @Override
    public void create()
    {
        awaitReady();
        super.create();
    }

    @Override
    public void update( Iterable<NodePropertyUpdate> updates )
    {
        awaitReady();
        super.update( updates );
    }

    @Override
    public void drop()
    {
        awaitReady();
        super.drop();
    }
    
    @Override
    public void ready()
    {
        latch.countDown();
        super.ready();
    }

    private void awaitReady()
    {
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    latch.await();
                    return;
                } catch (InterruptedException e) {
                    interrupted = true;
                    // fall through and retry
                }
            }
        } finally {
            if (interrupted)
                Thread.currentThread().interrupt();
        }
    }
}
