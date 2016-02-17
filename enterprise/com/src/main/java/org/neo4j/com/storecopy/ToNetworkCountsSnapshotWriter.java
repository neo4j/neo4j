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
package org.neo4j.com.storecopy;

import org.jboss.netty.buffer.ChannelBuffer;

import java.io.IOException;

import org.neo4j.com.ChannelBufferToChannel;
import org.neo4j.kernel.impl.store.counts.CountsSnapshot;
import org.neo4j.kernel.impl.store.counts.CountsSnapshotSerializer;

public class ToNetworkCountsSnapshotWriter implements SnapshotWriter
{
    private final ChannelBuffer targetBuffer;

    public ToNetworkCountsSnapshotWriter( ChannelBuffer targetBuffer )
    {
        this.targetBuffer = targetBuffer;
    }

    @Override
    public long write( CountsSnapshot snapshot ) throws IOException
    {
        return CountsSnapshotSerializer.serialize( new ChannelBufferToChannel( targetBuffer ), snapshot );
    }

}
