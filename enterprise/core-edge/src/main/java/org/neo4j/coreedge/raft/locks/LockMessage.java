/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.coreedge.raft.locks;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

import org.neo4j.kernel.ha.lock.LockResult;
import org.neo4j.kernel.impl.locking.Locks;

import static java.lang.String.format;

public interface LockMessage
{
    enum Action
    {
        NEW_SESSION, END_SESSION, ACQUIRE_EXCLUSIVE, ACQUIRE_SHARED
    }

    class Request implements Serializable
    {
        public final Action action;
        public final LockSession lockSession;
        public final Locks.ResourceType type;
        public final long[] resourceIds;

        private Request( Action action, LockSession lockSession, Locks.ResourceType type, long[] resourceIds )
        {
            this.action = action;
            this.lockSession = lockSession;
            this.type = type;
            this.resourceIds = resourceIds;
        }

        public static Request newLockSession(LockSession lockSession)
        {
            return new Request( Action.NEW_SESSION, lockSession, null, new long[0] );
        }

        public static Request endLockSession(LockSession lockSession)
        {
            return new Request( Action.END_SESSION, lockSession, null, new long[0] );
        }

        public static Request acquireExclusiveLock( LockSession lockSession, Locks.ResourceType type, long... resourceIds )
        {
            return new Request( Action.ACQUIRE_EXCLUSIVE, lockSession, type, resourceIds );
        }

        public static Request acquireSharedLock( LockSession lockSession, Locks.ResourceType type, long... resourceIds )
        {
            return new Request( Action.ACQUIRE_SHARED, lockSession, type, resourceIds );
        }

        @Override
        public String toString()
        {
            return format( "LockRequest{action=%s, lockSession=%s, type=%s, resourceIds=%s}",
                    action, lockSession, type, Arrays.toString( resourceIds ) );
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            Request request = (Request) o;
            return Objects.equals( lockSession, request.lockSession ) &&
                    Objects.equals( action, request.action ) &&
                    Objects.equals( type, request.type ) &&
                    Arrays.equals( resourceIds, request.resourceIds );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( action, lockSession, type, resourceIds );
        }
    }

    class Response
    {
        public final Request request;
        public final LockResult result;

        Response( Request request, LockResult result )
        {
            this.request = request;
            this.result = result;
        }

        @Override
        public String toString()
        {
            return format( "Response{request=%s, result=%s}", request, result );
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            Response response = (Response) o;
            return Objects.equals( request, response.request ) &&
                    Objects.equals( result, response.result );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( request, result );
        }
    }
}
