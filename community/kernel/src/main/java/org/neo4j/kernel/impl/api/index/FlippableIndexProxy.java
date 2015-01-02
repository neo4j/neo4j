/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.kernel.api.exceptions.index.ExceptionDuringFlipKernelException;
import org.neo4j.kernel.api.exceptions.index.FlipFailedKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexActivationFailedKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexProxyAlreadyClosedKernelException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.exceptions.schema.ConstraintVerificationFailedKernelException;

public class FlippableIndexProxy implements IndexProxy
{
    private boolean closed;
    private final ReadWriteLock lock = new ReentrantReadWriteLock( true );
    private IndexProxyFactory flipTarget;
    private IndexProxy delegate;

    public FlippableIndexProxy()
    {
        this( null );
    }

    public FlippableIndexProxy( IndexProxy originalDelegate )
    {
        this.delegate = originalDelegate;
    }

    @Override
    public void start() throws IOException
    {
        lock.readLock().lock();
        try
        {
            delegate.start();
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    @Override
    public IndexUpdater newUpdater( IndexUpdateMode mode )
    {
        // Making use of reentrant locks to ensure that the delegate's constructor is called under lock protection
        // while still retaining the lock until a call to close on the returned IndexUpdater
        lock.readLock().lock();
        try
        {
            return new LockingIndexUpdater( delegate.newUpdater( mode ) );
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    @Override
    public Future<Void> drop() throws IOException
    {
        lock.readLock().lock();
        try
        {
            closed = true;
            return delegate.drop();
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    @Override
    public void force() throws IOException
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
    public IndexDescriptor getDescriptor()
    {
        lock.readLock().lock();
        try
        {
            return delegate.getDescriptor();
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    @Override
    public SchemaIndexProvider.Descriptor getProviderDescriptor()
    {
        lock.readLock().lock();
        try
        {
            return delegate.getProviderDescriptor();
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

    @Override
    public Future<Void> close() throws IOException
    {
        lock.readLock().lock();
        try
        {
            closed = true;
            return delegate.close();
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    @Override
    public IndexReader newReader() throws IndexNotFoundKernelException
    {
        lock.readLock().lock();
        try
        {
            return delegate.newReader();
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    @Override
    public boolean awaitStoreScanCompleted() throws IndexPopulationFailedKernelException, InterruptedException
    {
        IndexProxy proxy;
        do
        {
            lock.readLock().lock();
            proxy = delegate;
            lock.readLock().unlock();
        } while ( proxy.awaitStoreScanCompleted() );
        return true;
    }

    @Override
    public void activate() throws IndexActivationFailedKernelException
    {
        // use write lock, since activate() might call flip*() which acquires a write lock itself.
        lock.writeLock().lock();
        try
        {
            delegate.activate();
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void validate() throws IndexPopulationFailedKernelException, ConstraintVerificationFailedKernelException
    {
        lock.readLock().lock();
        try
        {
            delegate.validate();
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    @Override
    public ResourceIterator<File> snapshotFiles() throws IOException
    {
        lock.readLock().lock();
        try
        {
            return delegate.snapshotFiles();
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    @Override
    public IndexPopulationFailure getPopulationFailure() throws IllegalStateException
    {
        lock.readLock().lock();
        try
        {
            return delegate.getPopulationFailure();
        }
        finally
        {
            lock.readLock().unlock();
        }
    }
    
    public void setFlipTarget( IndexProxyFactory flipTarget )
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

    public void flipTo( IndexProxy targetDelegate )
    {
        lock.writeLock().lock();
        try
        {
            this.delegate = targetDelegate;
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    public void flip( Callable<Void> actionDuringFlip, FailedIndexProxyFactory failureDelegate )
            throws FlipFailedKernelException
    {
        lock.writeLock().lock();
        try
        {
            assertStillOpenForBusiness();
            try
            {
                actionDuringFlip.call();
                this.delegate = flipTarget.create();
            }
            catch ( Exception e )
            {
                this.delegate = failureDelegate.create( e );
                throw new ExceptionDuringFlipKernelException( e );
            }
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + " -> " + delegate + "[target:" + flipTarget + "]";
    }

    private void assertStillOpenForBusiness() throws IndexProxyAlreadyClosedKernelException
    {
        if ( closed )
        {
            throw new IndexProxyAlreadyClosedKernelException( this.getClass() );
        }
    }

    private class LockingIndexUpdater extends DelegatingIndexUpdater
    {
        private LockingIndexUpdater( IndexUpdater delegate )
        {
            super( delegate );
            lock.readLock().lock();
        }

        @Override
        public void process( NodePropertyUpdate update ) throws IOException, IndexEntryConflictException
        {
            delegate.process( update );
        }

        @Override
        public void close() throws IOException, IndexEntryConflictException
        {
            try
            {
                delegate.close();
            }
            finally
            {
                lock.readLock().unlock();
            }
        }
    }
}
