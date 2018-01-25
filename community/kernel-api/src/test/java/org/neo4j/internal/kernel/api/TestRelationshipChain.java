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
package org.neo4j.internal.kernel.api;

import java.util.ArrayList;
import java.util.List;

class TestRelationshipChain
{
    private List<Data> data;
    private long originNodeId;

    TestRelationshipChain()
    {
        this.originNodeId = 42;
        this.data = new ArrayList<>();
    }

    TestRelationshipChain outgoing( long id, long targetNode, int type )
    {
        data.add( new Data( id, originNodeId, targetNode, type ) );
        return this;
    }

    TestRelationshipChain incoming( long id, long sourceNode, int type )
    {
        data.add( new Data( id, sourceNode, originNodeId, type ) );
        return this;
    }

    TestRelationshipChain loop( long id, int type )
    {
        data.add( new Data( id, originNodeId, originNodeId, type ) );
        return this;
    }

    Data get( int offset )
    {
        return data.get( offset );
    }

    boolean isValidOffset( int offset )
    {
        return offset >= 0 && offset < data.size();
    }

    long originNodeId()
    {
        return originNodeId;
    }

    static class Data
    {
        final long id;
        final long source;
        final long target;
        final int type;

        Data( long id, long source, long target, int type )
        {
            this.id = id;
            this.source = source;
            this.target = target;
            this.type = type;
        }
    }
}
