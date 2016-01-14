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
package org.neo4j.coreedge.server.core.locks;

/** Carries the currently valid token. */
public abstract class LockTokenManager
{
    /**
     *
     * @return The currently valid token.
     */
    public abstract LockToken currentToken();

    /**
     * Convenience method for retrieving a valid candidate id for a
     * lock token request.
     *
     *  @return A suitable candidate id for a token request.
     */
    public int nextCandidateId()
    {
        int candidateId = currentToken().id() + 1;
        if( candidateId == LockToken.INVALID_LOCK_TOKEN_ID )
        {
            candidateId++;
        }
        return candidateId;
    }

    /**
     * Waits for maximally the specified time for the awaitedId to be observed. Users of this interface must
     * still check afterwards that the current token is the sought one (i.e. check ownership).
     */
    public abstract void waitForTokenId( int awaitedId, long waitTimeMillis ) throws InterruptedException;
}
