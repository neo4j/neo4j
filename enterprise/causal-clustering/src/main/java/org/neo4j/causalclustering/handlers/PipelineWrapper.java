/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.handlers;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;

import java.util.List;

import static java.util.Collections.emptyList;

/**
 * Intended to provide handlers which can wrap an entire sub-pipeline in a neutral
 * fashion, e.g compression, integrity checks, ...
 */
public interface PipelineWrapper
{
    @SuppressWarnings( "RedundantThrows" )
    default List<ChannelHandler> handlersFor( Channel channel ) throws Exception
    {
        return emptyList();
    }

    String name();
}
