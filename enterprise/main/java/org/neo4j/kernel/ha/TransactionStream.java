/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.kernel.ha;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.util.Collection;

import org.neo4j.helpers.Pair;

/**
 * Represents a stream of the data of one or more consecutive transactions. 
 */
public final class TransactionStream
{
    private final Collection<Pair<Long, ReadableByteChannel>> channels;

    public TransactionStream( Collection<Pair<Long, ReadableByteChannel>> channels )
    {
        this.channels = channels;
    }

    public Collection<Pair<Long, ReadableByteChannel>> getChannels()
    {
        return channels;
    }

    public void close() throws IOException
    {
        for ( Pair<Long, ReadableByteChannel> channel : channels )
        {
            channel.other().close();
        }
    }
}
