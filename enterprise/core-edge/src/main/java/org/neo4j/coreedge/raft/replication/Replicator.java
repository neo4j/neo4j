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

/**
 * Replicate content across a cluster of servers.
 *
 * Content producers call {@link #replicate(ReplicatedContent)}.
 *
 * Content consumers {@link #subscribe(ReplicatedContentListener) subscribe} to replicated content,
 * and are notified when content is replicated from the same server, or from another server in the cluster.
 *
 * The actual delivery semantics of replicated content afforded to users depends on the underlying replicator
 * implementation.
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
     * Subscribe a listener which gets notified about all delivered replicated
     * content.
     *
     * @param listener The listener to subscribe.
     */
    void subscribe( ReplicatedContentListener listener );

    /**
     * Unsubscribe a previously registered replicated content listener.
     *
     * @param listener The listener to unsubscribe.
     */
    void unsubscribe( ReplicatedContentListener listener );

    interface ReplicatedContentListener
    {
        /**
         * Notification that content has been successfully replicated.
         *
         * @param content The replicated content.
         * @param logIndex The index of the content.
         */
        void onReplicated( ReplicatedContent content, long logIndex );
    }

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
