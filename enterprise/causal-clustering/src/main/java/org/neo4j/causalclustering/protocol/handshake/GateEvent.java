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
package org.neo4j.causalclustering.protocol.handshake;

import java.util.Objects;

public class GateEvent
{
    private final boolean isSuccess;

    private GateEvent( boolean isSuccess )
    {
        this.isSuccess = isSuccess;
    }

    private static GateEvent success = new GateEvent( true );
    private static GateEvent failure = new GateEvent( false );

    public static GateEvent getSuccess()
    {
        return success;
    }

    public static GateEvent getFailure()
    {
        return failure;
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
        GateEvent that = (GateEvent) o;
        return isSuccess == that.isSuccess;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( isSuccess );
    }
}
