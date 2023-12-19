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
package org.neo4j.kernel.ha.lock;

import java.util.Objects;

public class LockResult
{
    private final LockStatus status;
    private final String message;

    public LockResult( LockStatus status )
    {
        this.status = status;
        this.message = null;
    }

    public LockResult( LockStatus status, String message )
    {
        this.status = status;
        this.message = message;
    }

    public LockStatus getStatus()
    {
        return status;
    }

    public String getMessage()
    {
        return message;
    }

    @Override
    public String toString()
    {
        return "LockResult[" + status + ", " + message + "]";
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
        LockResult that = (LockResult) o;
        return Objects.equals( status, that.status ) &&
                Objects.equals( message, that.message );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( status, message );
    }
}
