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
package org.neo4j.coreedge.raft.log;

import org.neo4j.coreedge.raft.replication.ReplicatedContent;

/**
 * Persists entries that are coordinated through RAFT, i.e. this is the log
 * of user data.
 * <p/>
 * All write operations in this interface must be durably persisted before
 * returning from the respective functions.
 * <p/>
 * Entries are appended during the RAFT replication phase, and when safely
 * replicated they will be committed. The consumer of the raft entry log
 * can then safely apply the committed entry, typically to a state machine
 * with the entry representing a state transition to be performed.
 */
public interface RaftLog extends ReadableRaftLog
{
    String APPEND_INDEX_TAG = "appendIndex";
    String COMMIT_INDEX_TAG = "commitIndex";

    void replay() throws Throwable;

    void registerListener( Listener consumer );

    interface Listener
    {
        void onAppended( ReplicatedContent content );

        void onCommitted( ReplicatedContent content, long index );

        void onTruncated( long fromIndex );
    }

    /**
     * Appends entry to the end of the log. The first log index is 0.
     * <p/>
     * The entries must be uniquely identifiable and already appended
     * entries must not be re-appended (unless they have been removed
     * through truncation).
     *
     * @param entry The log entry.
     * @return Returns the index at which the entry was appended, or -1
     * if the entry was not accepted.
     */
    long append( RaftLogEntry entry ) throws RaftStorageException;

    /**
     * Truncates the log starting from the supplied index. Committed
     * entries can never be truncated.
     *
     * @param fromIndex The start index (inclusive).
     */
    void truncate( long fromIndex ) throws RaftStorageException;

    /**
     * Signals the safe replication of any entries previously appended up to and
     * including the supplied commitIndex. These entries can now be applied.
     * <p/>
     * The implementation must remember which committed entries have already been
     * applied so that they are not applied multiple times.
     *
     * @param commitIndex The end index (inclusive).
     */
    void commit( long commitIndex ) throws RaftStorageException;

}
