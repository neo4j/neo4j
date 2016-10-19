/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.causalclustering.core.replication.session;

import java.util.EmptyStackException;
import java.util.Stack;

/** Keeps a pool of local sub-sessions, to be used under a single global session. */
public class LocalSessionPool
{
    private final Stack<LocalSession> sessionStack = new Stack<>();

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
        LocalSession localSession;
        try
        {
            localSession = sessionStack.pop();
        }
        catch( EmptyStackException e )
        {
            localSession = createSession();
        }

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
