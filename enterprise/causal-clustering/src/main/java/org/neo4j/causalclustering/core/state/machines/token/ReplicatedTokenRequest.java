/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.core.state.machines.token;

import java.util.Arrays;
import java.util.function.Consumer;

import org.neo4j.causalclustering.core.state.CommandDispatcher;
import org.neo4j.causalclustering.core.state.Result;
import org.neo4j.causalclustering.core.state.machines.tx.CoreReplicatedContent;

public class ReplicatedTokenRequest implements CoreReplicatedContent
{
    private final TokenType type;
    private final String tokenName;
    private final byte[] commandBytes;

    public ReplicatedTokenRequest( TokenType type, String tokenName, byte[] commandBytes )
    {
        this.type = type;
        this.tokenName = tokenName;
        this.commandBytes = commandBytes;
    }

    public TokenType type()
    {
        return type;
    }

    String tokenName()
    {
        return tokenName;
    }

    byte[] commandBytes()
    {
        return commandBytes;
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

        ReplicatedTokenRequest that = (ReplicatedTokenRequest) o;

        if ( type != that.type )
        {
            return false;
        }
        if ( !tokenName.equals( that.tokenName ) )
        {
            return false;
        }
        return Arrays.equals( commandBytes, that.commandBytes );

    }

    @Override
    public int hashCode()
    {
        int result = type.hashCode();
        result = 31 * result + tokenName.hashCode();
        result = 31 * result + Arrays.hashCode( commandBytes );
        return result;
    }

    @Override
    public String toString()
    {
        return String.format( "ReplicatedTokenRequest{type='%s', name='%s'}",
                type, tokenName );
    }

    @Override
    public void dispatch( CommandDispatcher commandDispatcher, long commandIndex, Consumer<Result> callback )
    {
        commandDispatcher.dispatch( this, commandIndex, callback );
    }
}
