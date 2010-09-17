package org.neo4j.kernel.ha;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;


public abstract class ResourcePool<R>
{
    private static final boolean FAIR = true;

    private static class ResizableSemaphore extends Semaphore
    {
        private int permits;

        ResizableSemaphore( int permits )
        {
            super( permits, FAIR );
            this.permits = permits;
        }

        synchronized void setPermits( int permits )
        {
            if ( permits > this.permits )
            {
                release( permits - this.permits );
            }
            else if ( permits < this.permits )
            {
                reducePermits( this.permits - permits );
            }
            this.permits = permits;
        }
    }

    private final LinkedList<R> unused = new LinkedList<R>();
    private final Map<Thread, R> current = new ConcurrentHashMap<Thread, R>();
    private final ResizableSemaphore resources;
    private int maxUnused; // Guarded by unused

    protected ResourcePool( int maxResources, int maxUnused )
    {
        this.maxUnused = maxUnused;
        this.resources = new ResizableSemaphore( maxResources );
    }

    protected abstract R create();

    protected void dispose( R resource )
    {
    }

    protected boolean isAlive( R resource )
    {
        return true;
    }

    public final void setMaxResources( int maxResources )
    {
        resources.setPermits( maxResources );
    }

    public final R acquire()
    {
        Thread thread = Thread.currentThread();
        R resource = current.get( thread );
        if ( resource == null )
        {
            resources.acquireUninterruptibly();
            List<R> garbage = null;
            synchronized ( unused )
            {
                for ( ;; )
                {
                    resource = unused.poll();
                    if ( resource == null ) break;
                    if ( isAlive( resource ) ) break;
                    if ( garbage == null ) garbage = new LinkedList<R>();
                    garbage.add( resource );
                }
            }
            if ( resource == null )
            {
                resource = create();
            }
            current.put( thread, resource );
            if ( garbage != null )
            {
                for ( R dead : garbage )
                {
                    dispose( dead );
                }
            }
        }
        return resource;
    }

    public final void release()
    {
        Thread thread = Thread.currentThread();
        R resource = current.remove( thread );
        if ( resource != null )
        {
            boolean dead = false;
            synchronized ( unused )
            {
                if ( unused.size() < maxUnused )
                {
                    unused.add( resource );
                }
                else
                {
                    dead = true;
                }
            }
            resources.release();
            if ( dead ) dispose( resource );
        }
    }

    public final void close( boolean force )
    {
        List<R> dead = new LinkedList<R>();
        synchronized ( unused )
        {
            dead.addAll( unused );
            unused.clear();
            maxUnused = 0;
        }
        if ( force ) dead.addAll( current.values() );
        for ( R resource : dead )
        {
            dispose( resource );
        }
    }
}
