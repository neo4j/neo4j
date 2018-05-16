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
package org.neo4j.causalclustering.messaging.marshalling.v2;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.neo4j.function.ThrowingConsumer;
import org.neo4j.storageengine.api.WritableChannel;

public interface SerializableContent
{
    void serialize( WritableChannel channel ) throws IOException;

    static SimpleSerializableContent simple( byte contentType, ThrowingConsumer<WritableChannel,IOException> serializer )
    {
        return new SimpleSerializableContent( contentType, serializer );
    }

    class SimpleSerializableContent implements SerializableContent
    {
        private final byte contentType;
        private final ThrowingConsumer<WritableChannel,IOException> serializer;

        private SimpleSerializableContent( byte contentType, ThrowingConsumer<WritableChannel,IOException> serializer )
        {
            this.contentType = contentType;
            this.serializer = serializer;
        }

        public void serialize( WritableChannel channel ) throws IOException
        {
            channel.put( contentType );
            serializer.accept( channel );
        }
    }
}
