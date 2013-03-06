/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.index;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.neo4j.kernel.api.InternalIndexState;
import org.neo4j.kernel.api.KernelException;

public class FlippableIndexContext implements IndexContext
{
    public static final class FlipFailedKernelException extends KernelException
    {
        public FlipFailedKernelException( String message, Throwable cause )
        {
            super( message, cause );
        }
    }
    
    private static final Runnable NO_OP = new Runnable()
    {
        @Override
        public void run()
        {
        }
    };

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private IndexContextFactory flipTarget;
    private IndexContext delegate;

    public FlippableIndexContext()
    {
        this( null );
    }

    public FlippableIndexContext( IndexContext originalDelegate )
    {
        this.delegate = originalDelegate;
    }
    
    @Override
    public void create()
    {
        lock.readLock().lock();
        try
        {
            delegate.create();
        }
        finally
        {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public void update( Iterable<NodePropertyUpdate> updates )
    {
        lock.readLock().lock();
        try
        {
            delegate.update( updates );
        }
        finally
        {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public void drop()
    {
        lock.readLock().lock();
        try
        {
            delegate.drop();
        }
        finally
        {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public void force()
    {
        lock.readLock().lock();
        try
        {
            delegate.force();
        }
        finally
        {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public InternalIndexState getState()
    {
        lock.readLock().lock();
        try
        {
            return delegate.getState();
        }
        finally
        {
            lock.readLock().unlock();
        }
    }
    
    public IndexContext getDelegate()
    {
        return delegate;
    }

    public void setFlipTarget( IndexContextFactory flipTarget )
    {
        lock.writeLock().lock();
        try
        {
            this.flipTarget = flipTarget;
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    public void flip()
    {
        flip( NO_OP );
    }

    public void flip( Runnable actionDuringFlip )
    {
        lock.writeLock().lock();
        try
        {
            actionDuringFlip.run();
            this.delegate = flipTarget.create();
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }
    
    public void flip( Runnable actionDuringFlip, IndexContext failureFlipTarget ) throws FlipFailedKernelException
    {
        lock.writeLock().lock();
        try
        {
            actionDuringFlip.run();
            this.delegate = flipTarget.create();
        }
        catch( Exception e )
        {
            this.delegate = failureFlipTarget;
            throw new FlipFailedKernelException( "Failed to transition index to new context, see nested exception.", e );
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }
}
