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
    public abstract Buffer getBuffer();

    public abstract long position();

    public abstract int size();

    public abstract void force();

    public abstract void close();

    private OperationType type = null;
    private final FileChannel fileChannel;

    private Thread lockingThread = null;
    private final LinkedList<LockElement> waitingThreadList = 
        new LinkedList<LockElement>();
    private int lockCount = 0;
    private int marked = 0;

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
    
    protected abstract void writeOut();

    void setOperationType( OperationType type )
    {
        this.type = type;
    }

    synchronized void mark()
    {
        this.marked++;
    }

    synchronized boolean isMarked()
    {
        return marked > 0;
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

    synchronized int getWaitingThreadsCount()
    {
        return waitingThreadList.size();
    }
}