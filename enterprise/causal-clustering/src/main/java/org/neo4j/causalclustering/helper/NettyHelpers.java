/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.causalclustering.helper;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;

import java.util.Objects;

public class NettyHelpers
{
    public static int calculateChunkSize( ByteBufAllocator allocator, float saftyMargin, int defaultValue )
    {
        Objects.requireNonNull( allocator, "allocator cannot be null" );
        if ( 0 >= saftyMargin || 1 < saftyMargin )
        {
            throw new IllegalArgumentException( "safty magin must be > 0 and less than or equal to 1. Got" + saftyMargin );
        }
        if ( defaultValue <= 0 )
        {
            throw new IllegalArgumentException( "default value must be > 0. Got" + defaultValue );
        }
        if ( allocator instanceof PooledByteBufAllocator )
        {
            return (int) ((((PooledByteBufAllocator) allocator).metric().chunkSize()) * saftyMargin);
        }
        return defaultValue;
    }
}
