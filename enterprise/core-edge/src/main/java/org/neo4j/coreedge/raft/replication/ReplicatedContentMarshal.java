/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.coreedge.raft.replication;

public interface ReplicatedContentMarshal<T>
{
    /**
     * Serialize content into bytes
     *
     * @param content the content to serialize
     * @param buffer  the buffer to serialize into
     */
    void serialize( ReplicatedContent content, T buffer ) throws MarshallingException;

    /**
     * Deserialize content from a buffer. The buffer is consumed when this method returns.
     *
     * @param buffer the buffer to deserialize from
     * @return the deserialized content
     * @throws MarshallingException when the buffer cannot be correctly deserialized
     */
    ReplicatedContent deserialize( T buffer ) throws MarshallingException;
}
