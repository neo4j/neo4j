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
package org.neo4j.kernel.impl.nioneo.store;

import java.util.LinkedList;

import org.neo4j.kernel.impl.transaction.LockException;

/**
 * Makes a {@link PersistenceWindow} "lockable" meaning it can be locked by a
 * thread during a operation making sure no other thread use the same window
 * concurrently.
 */
public abstract class LockableWindow implements PersistenceWindow
{
    private final StoreChannel fileChannel;

    private Thread lockingThread = null;
    private final LinkedList<LockElement> waitingThreadList = 
        new LinkedList<LockElement>();
    private boolean locked;
    private int marked = 0;
    protected boolean closed;

    private boolean isDirty = false;

    LockableWindow( StoreChannel fileChannel )
    {
        this.fileChannel = fileChannel;
    }

    StoreChannel getFileChannel()
    {
        return fileChannel;
    }

    /**
     * Writes out any changes to the underlying {@link StoreChannel} and is then
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

    private static class LockElement
    {
        private Thread thread;
        private boolean movedOn = false;
        
        LockElement( Thread thread )
        {
            this.thread = thread;
        }
    }
    
    synchronized void lock( OperationType operationType )
    {
        Thread currentThread = Thread.currentThread();
        LockElement le = null;
        while ( locked && lockingThread != currentThread )
        {
            if (le == null)
                le = new LockElement( currentThread );

            waitingThreadList.addFirst( le );
            try
            {
                wait();
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
            }
        }
        locked = true;
        lockingThread = currentThread;

        if (le != null)
            le.movedOn = true;

        marked--;
        if ( operationType == OperationType.WRITE )
        {
            isDirty = true;
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

    synchronized void unLock()
    {
        Thread currentThread = Thread.currentThread();
        if ( !locked )
        {
            throw new LockException( currentThread
                + " doesn't have window lock on " + this );
        }
        locked = false;
        lockingThread = null;
        while ( !waitingThreadList.isEmpty() )
        {
            LockElement le = waitingThreadList.removeLast();
            if ( !le.movedOn )
            {
                le.thread.interrupt();
                break;
            }
        }
    }

    private boolean isFree( boolean assumingOwnerUnlockedIt )
    {
        return assumingOwnerUnlockedIt ?
                marked == 0 :           // excluding myself (the owner) no other must have marked this window
                marked == 0 && !locked; // no one must have this marked and it mustn't be locked
    }

    synchronized boolean writeOutAndCloseIfFree( boolean readOnly )
    {
        if ( isFree( lockingThread == Thread.currentThread() ) )
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
