/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.raft.replication.tx;

import java.util.Arrays;
import java.util.Optional;

import org.neo4j.coreedge.raft.state.CoreStateMachines;
import org.neo4j.coreedge.raft.state.Result;

public class ReplicatedTransaction implements CoreReplicatedContent
{
    private final byte[] txBytes;

    public ReplicatedTransaction( byte[] txBytes )
    {
        this.txBytes = txBytes;
    }

    public byte[] getTxBytes()
    {
        return txBytes;
    }

    @Override
    public Optional<Result> dispatch( CoreStateMachines coreStateMachines, long commandIndex )
    {
        return coreStateMachines.dispatch( this, commandIndex );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }
        ReplicatedTransaction that = (ReplicatedTransaction) o;
        return Arrays.equals( txBytes, that.txBytes );
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode( txBytes );
    }
}
