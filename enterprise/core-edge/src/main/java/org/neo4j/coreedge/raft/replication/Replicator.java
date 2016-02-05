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
package org.neo4j.coreedge.raft.replication;

/**
 * Replicate content across a cluster of servers.
 *
 * Content producers call {@link #replicate(ReplicatedContent)}.
 */
public interface Replicator
{
    /**
     * Submit content for replication. This method does not guarantee that the content
     * actually gets replicated, it merely makes an attempt at replication. Other
     * mechanisms must be used to achieve required delivery semantics.
     *
     * @param content The content to replicate.
     * @throws ReplicationFailedException Thrown when the replication surely failed.
     */
    void replicate( ReplicatedContent content ) throws ReplicationFailedException;

    /**
     * Thrown when the replication surely failed, as compared to cases
     * where the replication may or may not have succeeded, for which
     * cases this exception does not apply.
     */
    class ReplicationFailedException extends Exception
    {
        public ReplicationFailedException( Throwable cause )
        {
            super( cause );
        }

        public ReplicationFailedException( String message )
        {
            super( message );
        }
    }
}
