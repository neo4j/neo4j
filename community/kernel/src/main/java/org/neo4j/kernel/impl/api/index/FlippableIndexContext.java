package org.neo4j.kernel.impl.api.index;

public class FlippableIndexContext extends AbstractLockingIndexContext
{
    private static final Runnable NO_OP = new Runnable()
    {
        @Override
        public void run()
        {
        }
    };

    private IndexContext flipTarget = null;
    private IndexContext delegate;

    public FlippableIndexContext()
    {
        this( null );
    }

    public FlippableIndexContext( IndexContext originalDelegate )
    {
        this.delegate = originalDelegate;
    }

    public IndexContext getDelegate()
    {
        getLock().readLock().lock();
        try {
            return delegate;
        }
        finally {
             getLock().readLock().unlock();
        }
    }


    public void setFlipTarget( IndexContext flipTarget )
    {
        getLock().writeLock().lock();
        try {
            this.flipTarget = flipTarget;
        }
        finally {
            getLock().writeLock().unlock();
        }
    }

    public void flip()
    {
        flip( NO_OP );
    }

    public void flip( Runnable actionDuringFlip )
    {
        getLock().writeLock().lock();
        try {
            actionDuringFlip.run();
            this.delegate = flipTarget;
        }
        finally {
            getLock().writeLock().unlock();
        }
    }
}
