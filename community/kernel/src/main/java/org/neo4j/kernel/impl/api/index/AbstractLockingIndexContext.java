package org.neo4j.kernel.impl.api.index;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class AbstractLockingIndexContext<D extends IndexContext> extends AbstractDelegatingIndexContext<D>
{
    private final ReadWriteLock lock;

    public AbstractLockingIndexContext()
    {
        this.lock = new ReentrantReadWriteLock( );
    }

    protected ReadWriteLock getLock()
    {
        return lock;
    }

    public void create()
   {
       lock.readLock().lock();
       try {
           super.create();
       }
       finally {
           lock.readLock().unlock();
       }
   }

   public void update( Iterable<NodePropertyUpdate> updates )
   {
       lock.readLock().lock();
       try {
           super.update( updates );
       }
       finally {
           lock.readLock().unlock();
       }
   }

   public void drop()
   {
       lock.readLock().lock();
       try {
           super.drop();
       }
       finally {
           lock.readLock().unlock();
       }
   }
}
