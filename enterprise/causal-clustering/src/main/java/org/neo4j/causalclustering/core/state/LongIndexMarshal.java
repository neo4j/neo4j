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
package org.neo4j.causalclustering.core.state;

import java.io.IOException;

import org.neo4j.causalclustering.core.state.storage.SafeStateMarshal;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

/**
 * A marshal for an index that starts with -1 at the empty slot before the first real entry at 0.
 */
public class LongIndexMarshal extends SafeStateMarshal<Long>
{
    @Override
    public Long startState()
    {
        return -1L;
    }

    @Override
    public long ordinal( Long index )
    {
        return index;
    }

    @Override
    public void marshal( Long index, WritableChannel channel ) throws IOException
    {
        channel.putLong( index );
    }

    @Override
    protected Long unmarshal0( ReadableChannel channel ) throws IOException
    {
        return channel.getLong();
    }
}
