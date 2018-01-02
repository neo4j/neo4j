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
package org.neo4j.causalclustering.messaging.marshalling;

import java.nio.ByteBuffer;

/**
 * Implementations of this class perform marshalling (encoding/decoding) of instances of {@link STATE} into/from a
 * {@link ByteBuffer}.
 * @param <STATE> The class of objects supported by this marshal
 */
public interface ByteBufferMarshal<STATE>
{
    /**
     * Marshals the target into buffer. The buffer is expected to be large enough to hold the result.
     */
    void marshal( STATE state, ByteBuffer buffer );

    /**
     * Unmarshals an instance of {@link STATE} from source. If the source does not have enough bytes to fully read an
     * instance, null must be returned.
     */
    STATE unmarshal( ByteBuffer source );
}
