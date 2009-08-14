/*
 * Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.impl.nioneo.store;

import java.nio.channels.FileChannel;
import java.util.LinkedList;

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
    private final LinkedList<Thread> waitingThreadList = 
        new LinkedList<Thread>();
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

    synchronized void lock()
    {
        Thread currentThread = Thread.currentThread();
        while ( lockCount > 0 && lockingThread != currentThread )
        {
            waitingThreadList.addFirst( currentThread );
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
        marked--;
    }

    synchronized void unLock()
    {
        Thread currentThread = Thread.currentThread();
        if ( lockCount == 0 )
        {
            throw new RuntimeException( "" + currentThread
                + " don't have window lock on " + this );
        }
        lockCount--;
        if ( lockCount == 0 )
        {
            lockingThread = null;
            if ( waitingThreadList.size() > 0 )
            {
                waitingThreadList.removeLast().interrupt();
            }
        }
    }

    synchronized int getWaitingThreadsCount()
    {
        return waitingThreadList.size();
    }
}