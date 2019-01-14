/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.protocol.handshake;

import java.util.Objects;

public class InitialMagicMessage implements ServerMessage, ClientMessage
{
    // these can never, ever change
    static final String CORRECT_MAGIC_VALUE = "NEO4J_CLUSTER";
    static final int MESSAGE_CODE = 0x344F454E; // ASCII/UTF-8 "NEO4"

    private final String magic;
    // TODO: clusterId (String?)

    private static final InitialMagicMessage instance = new InitialMagicMessage( CORRECT_MAGIC_VALUE );

    InitialMagicMessage( String magic )
    {
        this.magic = magic;
    }

    public static InitialMagicMessage instance()
    {
        return instance;
    }

    @Override
    public void dispatch( ServerMessageHandler handler )
    {
        handler.handle( this );
    }

    @Override
    public void dispatch( ClientMessageHandler handler )
    {
        handler.handle( this );
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
        InitialMagicMessage that = (InitialMagicMessage) o;
        return Objects.equals( magic, that.magic );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( magic );
    }

    boolean isCorrectMagic()
    {
        return magic.equals( CORRECT_MAGIC_VALUE );
    }

    public String magic()
    {
        return magic;
    }

    @Override
    public String toString()
    {
        return "InitialMagicMessage{" + "magic='" + magic + '\'' + '}';
    }
}
