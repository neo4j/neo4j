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
package org.neo4j.io.pagecache.context;

/**
 * Context that contains state of ongoing versioned data read or write.
 *
 * <br/>
 * Context can be in one of two states:
 * <ul>
 *     <li>Read context: reading is performed for a version that context was initialised with.
 *     As soon as reader that associated with a context will observe data with version that it higher,
 *     context will be marked as dirty.
 *     <br/>
 *     For example, when context is initialised with last closed transaction id and if
 *     at the end of reading operation context is not marked as dirty its guarantee that context did not
 *     encounter any data from more recent transaction.</li>
 *     <li>Write context: context that performs data modifications. Any modifications will be tagged with
 *     some version that write context was initialised with.
 *     <br/>
 *     For example, commit will start write context that with a version that is equal to current
 *     committing transaction id.
 *     </li>
 * </ul>
 * By default, non context will be initialised with last closed transaction id which is equal to {@link Long#MAX_VALUE}
 * and transaction id that is equal to minimal possible transaction id: 1.
 *
 * Please note that contexts is snapshot engine use last closed transactions while multi versioned stores use highest close
 * and not visible ids to do version filtering.
 */
public interface VersionContext {
    /**
     * Initialise read context with the latest closed transaction id as it current version.
     */
    void initRead();

    /**
     * Initialise write context with committingTxId as modification version.
     * @param committingTxId currently committing transaction id
     */
    void initWrite(long committingTxId);

    /**
     * Context currently committing transaction id
     * @return committing transaction id
     */
    long committingTransactionId();

    /**
     * Last closed transaction id that read context was initialised with.
     * Used in snapshot execution engine as visibility guard.
     * @return last closed transaction id
     */
    long lastClosedTransactionId();

    /**
     * The highest closed tx id for this context. Together with array of not visible transactions ids
     * determines what versions of pages are visible for this context user.
     * Used in multi versioned stores as part of visibility criteria.
     */
    long highestClosed();

    /**
     * Mark current context as dirty
     */
    void markAsDirty();

    /**
     * Check whenever current context is dirty
     * @return true if context is dirty, false otherwise
     */
    boolean isDirty();

    /**
     * Array of not visible transaction with ids lower to the highest closed registered in a context.
     * Used in multi versioned stores as part of visibility criteria.
     */
    long[] notVisibleTransactionIds();

    /**
     * Global oldest visible transaction number at the time this context is initialized for write.
     * Any version lower than this one isn't visible by any active or future transaction and can be removed.
     */
    long oldestVisibleTransactionNumber();

    /**
     * Refresh cursor context visibility boundaries
     */
    void refreshVisibilityBoundaries();

    /**
     * Set invisible chain head version
     */
    void observedChainHead(long headVersion);

    /**
     * Check if this context ever encountered chain where latest visible page is not in the head of the chains
     */
    boolean invisibleHeadObserved();

    /**
     * Reset chain obsolete chain head state context
     */
    void resetObsoleteHeadState();

    /**
     * Mark observed head state as invisible
     */
    void markHeadInvisible();

    /**
     * Return version of the chain had that is invisible to current context.
     * If obsolete version was not encountered unspecified number if returned. To check
     * if obsolete version was encountered {@link #invisibleHeadObserved()} should be used.
     */
    long chainHeadVersion();

    boolean initializedForWrite();
}
