/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.store;

import org.neo4j.kernel.api.StatementConstants;

public class SingleNodeProgression implements StoreNodeCursor.Progression
{
    private long nodeId;

    SingleNodeProgression( long nodeId )
    {
        this.nodeId = nodeId;
    }

    @Override
    public long nextId()
    {
        try
        {
            return nodeId;
        }
        finally
        {
            nodeId = StatementConstants.NO_SUCH_NODE;
        }
    }

    @Override
    public Mode mode()
    {
        return Mode.FETCH;
    }
}
