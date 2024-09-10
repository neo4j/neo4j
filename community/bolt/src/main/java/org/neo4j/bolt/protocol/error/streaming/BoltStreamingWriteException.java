/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.protocol.error.streaming;

/**
 * Notifies a caller about an error condition in which the server is unable to write to its buffers or flush the buffer
 * contents to the underlying network connection while streaming a result set.
 */
public class BoltStreamingWriteException extends BoltStreamingException {
    public BoltStreamingWriteException(String message) {
        super(message);
    }

    public BoltStreamingWriteException(String message, Throwable cause) {
        super(message, cause);
    }
}
