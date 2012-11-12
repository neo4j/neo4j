/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.com;

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
        try
        {
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
                if ( dead ) dispose( resource );
            }
        }
        finally
        {
            resources.release();
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
