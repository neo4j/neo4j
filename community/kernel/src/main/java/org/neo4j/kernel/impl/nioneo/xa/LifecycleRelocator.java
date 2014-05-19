package org.neo4j.kernel.impl.nioneo.xa;

import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * The fact that this class exists is a smell. The sole reason is the have a {@link Lifecycle} be able
 * take change its position in the life compared to where it's instantiated.
 */
public class LifecycleRelocator implements Lifecycle
{
    private Lifecycle delegate;

    public void setDelegate( Lifecycle delegate )
    {
        this.delegate = delegate;
    }

    @Override
    public void init() throws Throwable
    {
        delegate.init();
    }

    @Override
    public void start() throws Throwable
    {
        delegate.start();
    }

    @Override
    public void stop() throws Throwable
    {
        delegate.stop();
    }

    @Override
    public void shutdown() throws Throwable
    {
        delegate.shutdown();
    }
}
