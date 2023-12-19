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
package org.neo4j.causalclustering.core.consensus.log;

import java.io.IOException;

/**
 * Persists entries that are coordinated through RAFT, i.e. this is the log
 * of user data.
 * <p/>
 * All write operations in this interface must be durably persisted before
 * returning from the respective functions.
 */
public interface RaftLog extends ReadableRaftLog
{
     String RAFT_LOG_DIRECTORY_NAME = "raft-log";

    /**
     * Appends entry to the end of the log. The first log index is 0.
     * <p/>
     * The entries must be uniquely identifiable and already appended
     * entries must not be re-appended (unless they have been removed
     * through truncation).
     *
     * @param entry The log entry.
     * @return the index at which the entry was appended.
     */
    long append( RaftLogEntry... entry ) throws IOException;

    /**
     * Truncates the log starting from the supplied index. Committed
     * entries can never be truncated.
     *
     * @param fromIndex The start index (inclusive).
     */
    void truncate( long fromIndex ) throws IOException;

    /**
     * Attempt to prune (delete) a prefix of the log, no further than the safeIndex.
     * <p/>
     * Implementations can choose to prune a shorter prefix if this is convenient for
     * their storage mechanism. The return value tells the caller how much was actually pruned.
     *
     * @param safeIndex Highest index that may be pruned.
     *
     * @return The new prevIndex for the log, which will be at most safeIndex.
     */
    long prune( long safeIndex ) throws IOException;

    /**
     * Skip up to the supplied index if it is not already present.
     * <p/>
     * If the entry was not present then it gets defined with the
     * supplied term, but without content, and thus can be used
     * only for log matching from a later index.
     * <p/>
     * This is useful when a snapshot starting from a later index
     * has been downloaded and thus earlier entries are irrelevant
     * and possibly non-existent in the cluster.
     *
     * @param index the index we want to skip to
     * @param term the term of the index
     *
     * @return The appendIndex after this call, which
     *         will be at least index.
     */
    long skip( long index, long term ) throws IOException;
}
