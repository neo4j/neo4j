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
package org.neo4j.coreedge.raft.state;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.coreedge.catchup.storecopy.core.RaftStateType;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;

public class StateMachines implements StateMachine
{
    private StateMachine[] machines;

    public StateMachines( StateMachine... machines )
    {
        this.machines = machines;
    }

    @Override
    public void applyCommand( ReplicatedContent content, long logIndex )
    {
        for ( StateMachine machine : machines )
        {
            machine.applyCommand( content, logIndex );
        }
    }

    @Override
    public void flush() throws IOException
    {
        for ( StateMachine machine : machines )
        {
            machine.flush();
        }
    }

    @Override
    public Map<RaftStateType, Object> snapshot()
    {
        HashMap<RaftStateType, Object> map = new HashMap<>();
        for ( StateMachine machine : machines )
        {
            map.putAll( machine.snapshot() );
        }
        return map;
    }

    @Override
    public void installSnapshot( Map<RaftStateType, Object> snapshot )
    {
        for ( StateMachine machine : machines )
        {
            machine.installSnapshot( snapshot );
        }
    }
}
