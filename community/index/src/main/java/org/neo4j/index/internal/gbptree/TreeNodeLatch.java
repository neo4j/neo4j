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
package org.neo4j.index.internal.gbptree;

/**
 * A read/write latch on a single tree node, handed out by a {@link TreeNodeLatchService} with one reference held.
 * While referenced the latch can be acquired for reading or writing.
 */
interface TreeNodeLatch {
    /**
     * @return the tree node id this latch guards.
     */
    long treeNodeId();

    /**
     * Releases the reference taken when this latch was handed out. May be called by any thread.
     */
    void deref();

    /**
     * Blocking call.
     * @return the read acquisition count this resulted in.
     */
    long acquireRead();

    /**
     * Non-blocking call.
     * @return the remaining read acquisition count.
     */
    long releaseRead();

    /**
     * Blocking call.
     */
    void acquireWrite();

    /**
     * Non-blocking call.
     */
    void releaseWrite();

    /**
     * Non-blocking call. Acquires the write latch if no reader or writer holds it.
     * @return whether the write latch was acquired.
     */
    boolean tryAcquireWrite();

    /**
     * Non-blocking call. Upgrades a held read acquisition to write.
     * @return whether the latch was upgraded. Returns {@code false} where waiting could deadlock, leaving the
     * read acquisition held.
     */
    boolean tryUpgradeToWrite();

    /**
     * Non-blocking call, for a thread holding a read latch.
     * @return whether the caller is currently the sole reader, with no writer.
     */
    boolean couldUpgradeToWrite();
}
