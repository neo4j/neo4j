/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.ha.lock;

public class LockResult
{
    private final LockStatus status;
    private final String deadlockMessage;
    
    public LockResult( LockStatus status )
    {
        this.status = status;
        this.deadlockMessage = null;
    }
    
    public LockResult( String deadlockMessage )
    {
        this.status = LockStatus.DEAD_LOCKED;
        this.deadlockMessage = deadlockMessage;
    }

    public LockStatus getStatus()
    {
        return status;
    }

    public String getDeadlockMessage()
    {
        return deadlockMessage;
    }
    
    @Override
    public String toString()
    {
        return "LockResult[" + status + ", " + deadlockMessage + "]";
    }
}
