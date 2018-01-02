/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.kernel.api.exceptions.index.ExceptionDuringFlipKernelException;
import org.neo4j.kernel.api.exceptions.index.FlipFailedKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexActivationFailedKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexCapacityExceededException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexProxyAlreadyClosedKernelException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintVerificationFailedKernelException;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.SchemaIndexProvider;

public class FlippableIndexProxy implements IndexProxy
{
    private volatile boolean closed;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock( true );
    private volatile IndexProxyFactory flipTarget;
    // This variable below is volatile because it can be changed in flip or flipTo
    // and even though it may look like acquiring the read lock, when using this variable
    // for various things, execution flow would go through a memory barrier of some sort.
    // But it turns out that that may not be the case. F.ex. ReentrantReadWriteLock
    // code uses unsafe compareAndSwap that sort of circumvents an equivalent of a volatile read.
    private volatile IndexProxy delegate;

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

    /**
     * The {@code force()}-method is called during log rotation. At this time we do not want to wait for locks held by
     * {@link LockingIndexUpdater}. Waiting on such locks would cause a serious risk of deadlocks, since very likely
     * the reader we would be waiting on would be waiting on the log rotation lock held by the thread calling this
     * method. The reason we would wait for a read lock while trying to acquire a read lock is if there is a third
     * thread waiting on the write lock, probably an index populator wanting to
     * {@linkplain #flip(Callable, FailedIndexProxyFactory) flip the index into active state}.
     * <p/>
     * We avoid this deadlock situation by "barging" on the read lock, i.e. acquire it in an <i>unfair</i> way, where
     * we don't care about waiting threads, only about whether the exclusive lock is held or not.
     */
    @Override
    public void force() throws IOException
    {
        barge( lock.readLock() ); // see javadoc of this method (above) for rationale on why we use barge(...) here
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
    public void flush() throws IOException
    {
        barge( lock.readLock() ); // see javadoc of this method (above) for rationale on why we use barge(...) here
        try
        {
            delegate.flush();
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /**
     * Acquire the {@code ReadLock} in an <i>unfair</i> way, without waiting for queued up writers.
     * <p/>
     * The {@link ReentrantReadWriteLock.ReadLock#tryLock() tryLock}-method of the {@code ReadLock} implementation of
     * {@code ReentrantReadWriteLock} implements a <i>barging</i> behaviour, where if an exclusive lock is not held,
     * the shared lock will be acquired, even if there are other threads waiting for the lock. This behaviour is
     * regardless of whether the lock is fair or not.
     * <p/>
     * This allows us to avoid deadlocks where readers would wait for writers that wait for readers in critical
     * methods.
     * <p/>
     * The naive way to implement this method would be:
     * <pre><code>
     *     if ( !lock.tryLock() ) // try to barge
     *         lock.lock(); // fall back to normal blocking lock call
     * </code></pre>
     * This would however not implement the appropriate barging behaviour in a scenario like the following: Say the
     * exclusive lock is held, and there is a queue waiting containing first a reader and then a writer, in this case
     * the {@code tryLock()} method will return false. If the writer then finishes between the naive implementation
     * exiting {@code tryLock()} and before entering {@code lock()} the {@code barge(...)} method would now block in
     * the exact way we don't want it to block, with a read lock held and a writer waiting.<br/>
     * In order to get around this situation, the implementation of this method uses a
     * {@linkplain Lock#tryLock(long, TimeUnit) timed wait} in a retry-loop in order to ensure that we make another
     * attempt to barge the lock at a later point.
     * <p/>
     * This method is written to be compatible with the signature of {@link Lock#lock()}, which is not interruptible,
     * but implemented based on the interruptible {@link Lock#tryLock(long, TimeUnit)}, so the implementation needs to
     * remember being interrupted, and reset the flag before exiting, so that later invocations of interruptible
     * methods detect the interruption.
     *
     * @param lock a {@link java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock}
     */
    private static void barge( ReentrantReadWriteLock.ReadLock lock )
    {
        boolean interrupted = false;
        // exponential retry back-off, no more than 1 second
        for ( long timeout = 10; !lock.tryLock(); timeout = Math.min( 1000, timeout * 2 ) )
        {
            try
            {
                if ( lock.tryLock( timeout, TimeUnit.MILLISECONDS ) )
                {
                    return;
                }
            }
            // the barge()-method is uninterruptable, but implemented based on the interruptible tryLock()-method
            catch ( InterruptedException e )
            {
                Thread.interrupted(); // ensure the interrupt flag is cleared
                interrupted = true; // remember to set interrupt flag before we exit
            }
        }
        if ( interrupted )
        {
            Thread.currentThread().interrupt(); // reset the interrupt flag
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
    public IndexConfiguration config()
    {
        lock.readLock().lock();
        try
        {
            return delegate.config();
        }
        finally
        {
            lock.readLock().unlock();
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
        public void close() throws IOException, IndexEntryConflictException, IndexCapacityExceededException
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
