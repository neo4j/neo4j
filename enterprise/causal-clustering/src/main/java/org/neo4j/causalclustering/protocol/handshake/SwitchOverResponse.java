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

public class SwitchOverResponse implements ClientMessage
{
    public static final SwitchOverResponse FAILURE = new SwitchOverResponse( StatusCode.FAILURE );
    private final StatusCode status;

    SwitchOverResponse( StatusCode status )
    {
        this.status = status;
    }

    @Override
    public void dispatch( ClientMessageHandler handler )
    {
        handler.handle( this );
    }

    public StatusCode status()
    {
        return status;
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
        SwitchOverResponse that = (SwitchOverResponse) o;
        return status == that.status;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( status );
    }

    @Override
    public String toString()
    {
        return "SwitchOverResponse{" + "status=" + status + '}';
    }
}
