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
package org.neo4j.causalclustering.messaging.marshalling;

import java.io.IOException;

import org.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

/**
 * Implementations of this class perform marshalling (encoding/decoding) of {@link STATE}
 * into/from a {@link WritableChannel} and a {@link ReadableChannel} respectively.
 *
 * N.B.: Implementations should prefer to extend {@link SafeChannelMarshal} to handle
 * {@link org.neo4j.storageengine.api.ReadPastEndException} correctly.
 *
 * @param <STATE> The class of objects supported by this marshal
 */
public interface ChannelMarshal<STATE>
{
    /**
     * Marshals the state into the channel.
     */
    void marshal( STATE state, WritableChannel channel ) throws IOException;

    /**
     * Unmarshals an instance of {@link STATE} from channel. If the channel does not have enough bytes
     * to fully read an instance then an {@link EndOfStreamException} must be thrown.
     *
     * N.B: The ReadableChannel is sort of broken in its implementation and throws
     * {@link org.neo4j.storageengine.api.ReadPastEndException} which is a subclass of IOException
     * and that is problematic since usually the case of reaching the end of a stream actually
     * requires handling distinct from that of arbitrary IOExceptions. Although it was possible
     * to catch that particular exception explicitly, you would not get compiler/IDE support
     * for making that apparent.
     */
    STATE unmarshal( ReadableChannel channel ) throws IOException, EndOfStreamException;
}
