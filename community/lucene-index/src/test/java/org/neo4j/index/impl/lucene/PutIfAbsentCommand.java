/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.index.impl.lucene;

import org.neo4j.graphdb.Node;
import org.neo4j.test.OtherThreadExecutor.WorkerCommand;

public class PutIfAbsentCommand implements WorkerCommand<CommandState, Node>
{
    private final String key;
    private final Object value;
    private final Node node;

    public PutIfAbsentCommand( Node node, String key, Object value )
    {
        this.node = node;
        this.key = key;
        this.value = value;
    }

    @Override
    public Node doWork( CommandState state )
    {
        return state.index.putIfAbsent( node, key, value );
    }
}
