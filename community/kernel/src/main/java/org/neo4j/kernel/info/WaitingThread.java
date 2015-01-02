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
package org.neo4j.kernel.info;

import java.beans.ConstructorProperties;

public final class WaitingThread extends LockingTransaction
{
    public static WaitingThread create( String transaction, int readCount, int writeCount, Thread thread,
                                        long waitingSince, boolean isWriteLock )
    {
        return new WaitingThread( transaction, readCount, writeCount, thread.getId(), thread.getName(), waitingSince,
                isWriteLock );
    }

    private static final long serialVersionUID = 1L;
    private final boolean writeLock;
    private final long threadId;
    private final String threadName;
    private final long waitingSince;

    @ConstructorProperties({"transaction", "readCount", "writeCount", "threadId", "threadName", "waitingSince",
            "waitingOnWriteLock"})
    public WaitingThread( String transaction, int readCount, int writeCount, long threadId, String threadName,
                          long waitingSince, boolean writeLock )
    {
        super( transaction, readCount, writeCount );
        this.threadId = threadId;
        this.threadName = threadName;
        this.waitingSince = waitingSince;
        this.writeLock = writeLock;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append( "\"" ).append( getThreadName() ).append( "\" " ).append( " (" ).append( getTransaction() ).
                append( ") " ).append( "[tid=" ).append( getThreadId() ).append( "(" ).append(
                getReadCount() ).append( "r," ).append( getWriteCount() ).append( "w )," ).append(
                isWaitingOnWriteLock() ? "Write" : "Read" ).append( "Lock]" );

        return builder.toString();
    }

    public boolean isWaitingOnReadLock()
    {
        return !writeLock;
    }

    public boolean isWaitingOnWriteLock()
    {
        return writeLock;
    }

    public long getThreadId()
    {
        return threadId;
    }

    public String getThreadName()
    {
        return threadName;
    }

    public long getWaitingSince()
    {
        return waitingSince;
    }
}
