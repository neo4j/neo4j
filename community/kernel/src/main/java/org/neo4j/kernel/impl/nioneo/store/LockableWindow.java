/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.store;

import java.nio.channels.FileChannel;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Makes a {@link PersistenceWindow} "lockable" meaning it can be locked by a
 * thread during a operation making sure no other thread use the same window
 * concurrently.
 * <p>
 * The synchronization policy is thus: internal state changes that need to preserve
 * invariants, must be protected by the monitor lock, i.e. with the
 * <code>synchronized</code> keyword. The locking mechanism that the LockableWindow
 * itself exposes, where threads are allowed to wait for longer periods of time,
 * is implemented using a java.util.concurrent.lock.ReentrantLock. This lock must
 * not be tried while the monitor lock is held, because then a stall on the
 * ReentrantLock can propagate to the monitor.
 */
abstract class LockableWindow implements PersistenceWindow
{
    private final ReentrantLock lock = new ReentrantLock();
    private final FileChannel fileChannel;

    private int marked = 0;
    protected boolean closed;

    private boolean isDirty = false;

    LockableWindow( FileChannel fileChannel )
    {
        this.fileChannel = fileChannel;
    }

    boolean encapsulates( long position )
    {
        return position() <= position && position < position() + size();
    }

    FileChannel getFileChannel()
    {
        return fileChannel;
    }

    /**
     * Writes out any changes to the underlying {@link FileChannel} and is then
     * considered unusable.
     */
    protected final void writeOutAndClose()
    {
        force();
        close();
    }

    /**
     * @return {@code true} if marked, or {@code false} if this window has been
     * closed and couldn't be marked.
     */
    synchronized boolean markAsInUse()
    {
        if ( closed )
            return false;
        this.marked++;
        return true;
    }

    void lock( OperationType operationType )
    {
        lock.lock();
        synchronized ( this )
        {
            marked--;
            if ( operationType == OperationType.WRITE )
            {
                isDirty = true;
            }
        }
    }
    
    synchronized boolean isDirty()
    {
        return isDirty;
    }

    synchronized void setClean()
    {
        isDirty = false;
    }

    void unLock()
    {
        lock.unlock();
    }

    private boolean isFree()
    {
        return lock.isHeldByCurrentThread() ?
                marked == 0 :           // excluding myself (the owner) no other must have marked this window
                marked == 0 && !lock.isLocked(); // no one must have this marked and it mustn't be locked
    }

    synchronized boolean writeOutAndCloseIfFree( boolean readOnly )
    {
        if ( isFree() )
        {
            if ( !readOnly )
                writeOutAndClose();
            return true;
        }
        return false;
    }

    /**
     * Accepts and applies contents from a {@link PersistenceRow}.
     * @param dpw the {@link PersistenceRow} to accept changes from.
     */
    void acceptContents( PersistenceRow dpw )
    {
        throw new UnsupportedOperationException( "Should not be called on " + this + " which is a " + getClass() );
    }
}
