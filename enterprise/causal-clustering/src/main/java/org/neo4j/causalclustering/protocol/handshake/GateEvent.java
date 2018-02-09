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
