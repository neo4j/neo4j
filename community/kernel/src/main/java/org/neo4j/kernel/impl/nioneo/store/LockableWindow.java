/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
import java.util.LinkedList;

import org.neo4j.kernel.impl.transaction.LockException;

/**
 * Makes a {@link PersistenceWindow} "lockable" meaning it can be locked by a
 * thread during a operation making sure no other thread use the same window
 * concurrently.
 */
abstract class LockableWindow implements PersistenceWindow
{
    private OperationType type = null;
    private final FileChannel fileChannel;

    private Thread lockingThread = null;
    private final LinkedList<LockElement> waitingThreadList = 
        new LinkedList<LockElement>();
    private int lockCount = 0;
    private int marked = 0;
    protected boolean closed;

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

    OperationType getOperationType()
    {
        return type;
    }
    
    /**
     * Writes out any changes to the underlying {@link FileChannel} and is then
     * considered unusable.
     */
    protected abstract void writeOutAndClose();

    void setOperationType( OperationType type )
    {
        this.type = type;
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
        private final Thread thread;
        private boolean movedOn = false;
        
        LockElement( Thread thread )
        {
            this.thread = thread;
        }
    }
    
    synchronized void lock()
    {
        Thread currentThread = Thread.currentThread();
        LockElement le = new LockElement( currentThread );
        while ( lockCount > 0 && lockingThread != currentThread )
        {
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
        lockCount++;
        lockingThread = currentThread;
        le.movedOn = true;
        marked--;
    }

    synchronized void unLock()
    {
        Thread currentThread = Thread.currentThread();
        if ( lockCount == 0 )
        {
            throw new LockException( "" + currentThread
                + " don't have window lock on " + this );
        }
        lockCount--;
        if ( lockCount == 0 )
        {
            lockingThread = null;
            if ( waitingThreadList.size() > 0 )
            {
                LockElement le = waitingThreadList.removeLast();
                if ( !le.movedOn )
                {
                    le.thread.interrupt();
                }
            }
        }
    }

    synchronized boolean isFree()
    {
        return waitingThreadList.isEmpty() && marked == 0;
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