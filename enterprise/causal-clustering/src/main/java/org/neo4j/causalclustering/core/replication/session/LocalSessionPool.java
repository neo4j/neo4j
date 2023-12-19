/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.core.replication.session;

import java.util.ArrayDeque;
import java.util.Deque;

/** Keeps a pool of local sub-sessions, to be used under a single global session. */
public class LocalSessionPool
{
    private final Deque<LocalSession> sessionStack = new ArrayDeque<>();

    private final GlobalSession globalSession;
    private long nextLocalSessionId;

    public LocalSessionPool( GlobalSession globalSession )
    {
        this.globalSession = globalSession;
    }

    private LocalSession createSession()
    {
        return new LocalSession( nextLocalSessionId++ );
    }

    public GlobalSession getGlobalSession()
    {
        return globalSession;
    }

    /**
     * Acquires a session and returns the next unique operation context
     * within that session. The session must be released when the operation
     * has been successfully finished. */
    public synchronized OperationContext acquireSession()
    {
        final LocalSession localSession = sessionStack.isEmpty() ? createSession() : sessionStack.pop();
        return new OperationContext( globalSession, localSession.nextOperationId(), localSession );
    }

    /**
     * Releases a previously acquired session using the operation context
     * as a key. An unsuccessful operation should not be released, but it
     * will leak a local session.
     *
     * The reason for not releasing an unsuccessful session is that operation
     * handlers might restrict sequence numbers to occur in strict order, and
     * thus an operation that it hasn't handled will block any future
     * operations under that session.
     *
     * In general all operations should be retried until they do succeed, or
     * the entire session manager should eventually be restarted, thus
     * allocating a new global session to operate under.
     */
    public synchronized void releaseSession( OperationContext operationContext )
    {
        sessionStack.push( operationContext.localSession() );
    }

    public synchronized long openSessionCount()
    {
        return nextLocalSessionId - sessionStack.size();
    }
}
