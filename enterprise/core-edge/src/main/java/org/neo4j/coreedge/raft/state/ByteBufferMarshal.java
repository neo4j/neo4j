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
package org.neo4j.coreedge.raft.state;

import java.nio.ByteBuffer;

/**
 * This interface defines the ability of a class to marshal instances of another class to and from a ByteBuffer.
 * @param <TARGET> The class being marshaled.
 */
public interface ByteBufferMarshal<TARGET>
{
    /**
     * Marshals the target into buffer. The buffer is expected to be large enough to hold the result.
     */
    void marshal( TARGET target, ByteBuffer buffer );

    /**
     * Unmarshals an instance of TARGET from source. If the source does not have enough bytes to fully read an instance,
     * null must be returned.
     */
    TARGET unmarshal( ByteBuffer source );
}
