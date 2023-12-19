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
