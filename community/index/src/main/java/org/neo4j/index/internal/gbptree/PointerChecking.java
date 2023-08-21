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

import org.neo4j.io.pagecache.PageCursor;

/**
 * Methods for ensuring a read {@link GenerationSafePointer GSP pointer} is valid.
 */
class PointerChecking {
    static final String WRITER_TRAVERSE_OLD_STATE_MESSAGE =
            "Writer traversed to a tree node that has a valid successor, "
                    + "This is most likely due to failure to checkpoint the tree before shutdown and/or tree state "
                    + "being out of date.";

    private PointerChecking() {}

    /**
     * Used by tests only that don't care about exception message so much.
     */
    static void checkPointer(long result, boolean allowNoNode) {
        checkPointer(result, allowNoNode, -1, "unknown", 0, 1);
    }

    /**
     * NOTE! If using a read cursor, please use {@link #checkPointer(long, boolean, long, String, long, long)} instead.
     * <p>
     * Checks a read pointer for success/failure and throws appropriate exception with failure information
     * if failure. Must be called after a consistent read from page cache (after {@link PageCursor#shouldRetry()}.
     *
     * @param result result from {@link GenerationSafePointerPair#FLAG_READ} or
     * {@link GenerationSafePointerPair#write(PageCursor, long, long, long)}.
     * @param allowNoNode If {@link TreeNodeUtil#NO_NODE_FLAG} is allowed as pointer value.
     * @param nodeId id of node from which result was read.
     * @param pointerType a string describing the type of pointer that was read.
     * @param stableGeneration current stable generation.
     * @param unstableGeneration current unstable generation.
     * @param cursor cursor pinned to the node from which result was read. Cursor will be used to read additional information from tree to construct
     * exception message if needed.
     * @param offset offset in node from which result was read.
     */
    static void checkPointer(
            long result,
            boolean allowNoNode,
            long nodeId,
            String pointerType,
            long stableGeneration,
            long unstableGeneration,
            PageCursor cursor,
            int offset) {
        GenerationSafePointerPair.assertSuccess(
                result, nodeId, pointerType, stableGeneration, unstableGeneration, cursor, offset);
        assertIdSpace(allowNoNode, result);
    }

    /**
     * See {@link #checkPointer(long, boolean, long, String, long, long, PageCursor, int)} but without cursor and offset.
     */
    static void checkPointer(
            long result,
            boolean allowNoNode,
            long nodeId,
            String pointerType,
            long stableGeneration,
            long unstableGeneration) {
        GenerationSafePointerPair.assertSuccess(result, nodeId, pointerType, stableGeneration, unstableGeneration);
        assertIdSpace(allowNoNode, result);
    }

    private static void assertIdSpace(boolean allowNoNode, long result) {
        if (allowNoNode && !TreeNodeUtil.isNode(result)) {
            return;
        }
        if (result < IdSpace.MIN_TREE_NODE_ID) {
            throw new TreeInconsistencyException(
                    "Pointer to id " + result + " not allowed. Minimum node id allowed is " + IdSpace.MIN_TREE_NODE_ID);
        }
    }

    /**
     * Assert cursor rest on a node that does not have a valid (not crashed) successor.
     *
     * @param cursor PageCursor resting on a tree node.
     * @param stableGeneration Current stable generation of tree.
     * @param unstableGeneration Current unstable generation of tree.
     */
    static boolean assertNoSuccessor(PageCursor cursor, long stableGeneration, long unstableGeneration) {
        long successor = TreeNodeUtil.successor(cursor, stableGeneration, unstableGeneration);
        if (TreeNodeUtil.isNode(successor)) {
            throw new TreeInconsistencyException(WRITER_TRAVERSE_OLD_STATE_MESSAGE);
        }
        return true;
    }

    /**
     * Calls {@link PageCursor#checkAndClearBoundsFlag()} and if {@code true} throws {@link TreeInconsistencyException}.
     * Should be called whenever leaving a {@link PageCursor#shouldRetry() shouldRetry-loop} successfully.
     * Purpose of this method is to unify {@link PageCursor} read behavior and exception handling.
     *
     * @param cursor {@link PageCursor} to check for out-of-bounds.
     */
    static void checkOutOfBounds(PageCursor cursor) {
        if (cursor.checkAndClearBoundsFlag()) {
            throw new TreeInconsistencyException(
                    "Some internal problem causing out of bounds: pageId:" + cursor.getCurrentPageId());
        }
    }
}
