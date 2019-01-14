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
