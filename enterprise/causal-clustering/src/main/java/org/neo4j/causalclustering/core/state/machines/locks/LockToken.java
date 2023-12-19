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
package org.neo4j.causalclustering.core.state.machines.locks;

/**
 * Represents a lock token by an pairing an id together with its owner. A lock token
 * is held by a single server at any point in time and the token holder is the
 * only one that should be using locks. It is used as an ordering primitive
 * in the consensus machinery to mark local lock validity by using it as
 * the cluster lock session id.
 *
 * The reason for calling it a token is to clarify the fact that there logically
 * is just a single valid token at any point in time, which gets requested and
 * logically passed around. When bound to a transaction the id gets used as a
 * lock session id in the cluster.
 */
interface LockToken
{
    int INVALID_LOCK_TOKEN_ID = -1;

    /**
     * Convenience method for retrieving a valid candidate id for a
     * lock token request.
     *
     *  @return A suitable candidate id for a token request.
     * @param currentId
     */
    static int nextCandidateId( int currentId )
    {
        int candidateId = currentId + 1;
        if ( candidateId == INVALID_LOCK_TOKEN_ID )
        {
            candidateId++;
        }
        return candidateId;
    }

    /**
     * The id of the lock token.
     */
    int id();

    /**
     * The owner of this lock token.
     */
    Object owner();
}
