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
package org.neo4j.causalclustering.messaging.marshalling.v2.decoding;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.util.List;

import org.neo4j.causalclustering.catchup.Protocol;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.messaging.marshalling.v2.ContentBuilder;
import org.neo4j.causalclustering.messaging.marshalling.v2.ContentType;

public class ReplicatedContentDecoder extends MessageToMessageDecoder<ContentBuilder<ReplicatedContent>>
{
    private final Protocol<ContentType> protocol;
    private ContentBuilder<ReplicatedContent> contentBuilder = ContentBuilder.emptyUnfinished();

    public ReplicatedContentDecoder( Protocol<ContentType> protocol )
    {
        this.protocol = protocol;
    }

    @Override
    protected void decode( ChannelHandlerContext ctx, ContentBuilder<ReplicatedContent> msg, List<Object> out )
    {
        contentBuilder.combine( msg );
        if ( contentBuilder.isComplete() )
        {
            out.add( contentBuilder.build() );
            contentBuilder = ContentBuilder.emptyUnfinished();
            protocol.expect( ContentType.MessageType );
        }
    }
}
