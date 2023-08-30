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
import org.neo4j.io.pagecache.context.CursorContext;

/**
 * Methods for (binary-)searching keys in a tree node.
 */
class KeySearch {

    private KeySearch() {}

    /**
     * Search for left most pos such that keyAtPos obeys key <= keyAtPos.
     * Return pos (not offset) of keyAtPos, or key count if no such key exist.
     * <p>
     * On insert, key should be inserted at pos.
     * On seek in internal, child at pos should be followed from internal node.
     * On seek in leaf, value at pos is correct if keyAtPos is equal to key.
     * <p>
     * Implemented as binary search.
     * <p>
     * Leaves cursor on same page as when called. No guarantees on offset.
     *
     * @param cursor    {@link PageCursor} pinned to page with node (internal or leaf does not matter)
     * @param node      {@link SharedNodeBehaviour} that knows how to operate on KEY
     * @param key       KEY to search for
     * @param readKey   KEY to use as temporary storage during calculation.
     * @param keyCount  number of keys in node when starting search
     * @return position of the leftmost key that is equal to the requested key, if it found;
     * otherwise <code>(-(<i>insertion point</i>) - 1)</code>.
     * The insertion point is the first i such that
     * node.keyComparator().compare( key, node.keyAt( i ) < 0, or keyCount if no such key exists.
     * To extract position from the returned search result, then use {@link #positionOf(int)}.
     * To extract whether the exact key was found, then use {@link #isHit(int)}.
     */
    static <KEY> int search(
            PageCursor cursor,
            SharedNodeBehaviour<KEY> node,
            KEY key,
            KEY readKey,
            int keyCount,
            CursorContext cursorContext) {
        if (keyCount == 0) {
            return -1;
        }

        // Compare key with lower and higher and sort out special cases
        var comparator = node.keyComparator();
        // key greater than or equal the greatest key in node
        if ((comparator.compare(key, node.keyAt(cursor, readKey, keyCount - 1, cursorContext))) > 0) {
            return -keyCount - 1;
        }
        {
            // key smaller than or equal to the smallest key in node
            int comparison;
            if ((comparison = comparator.compare(key, node.keyAt(cursor, readKey, 0, cursorContext))) <= 0) {
                if (comparison == 0) {
                    return 0;
                }
                // insertion point is 0
                return -1;
            }
        }

        // Start binary search
        // If key <= keyAtPos -> move higher to pos
        // If key > keyAtPos -> move lower to pos+1
        // Terminate when lower == higher
        int lower = 0;
        int higher = keyCount - 1;
        int pos;
        while (lower < higher) {
            pos = (lower + higher) / 2;
            int comparison = comparator.compare(key, node.keyAt(cursor, readKey, pos, cursorContext));
            if (comparison <= 0) {
                higher = pos;
            } else {
                lower = pos + 1;
            }
        }
        pos = lower;

        if (comparator.compare(key, node.keyAt(cursor, readKey, pos, cursorContext)) == 0) {
            return pos;
        }
        return -(pos + 1);
    }

    /**
     * Extracts the position from a search result from {@link #search(PageCursor, SharedNodeBehaviour, Object, Object, int, CursorContext)}
     * Note! If position will be used as position for child pointer, use {@link #childPositionOf(int)} instead.
     *
     * @param searchResult search result
     * @return position of the search result.
     */
    static int positionOf(int searchResult) {
        if (searchResult >= 0) {
            return searchResult;
        }
        return -searchResult - 1;
    }

    /**
     * Extracts the position from a search result from {@link #search(PageCursor, SharedNodeBehaviour, Object, Object, int, CursorContext)}.
     * Because the extracted position will be used as position for child pointer we need
     * to take care of the special case where we had an exact match on the key. This is why:
     * - KeySearch find the left most pos such that keyAtPos obeys key <= keyAtPos.
     * - We want to follow the child pointer to the left of keyAtPos unless key == keyAtPos,
     *   in which case we want to follow the pointer to the right. This is of course because everything
     *   larger than _or equal_ to key belongs to right subtree.
     */
    static int childPositionOf(int searchResult) {
        if (searchResult >= 0) {
            return searchResult + 1;
        }
        return -searchResult - 1;
    }

    /**
     * Extracts whether the searched key was found from search result from {@link #search(PageCursor, SharedNodeBehaviour, Object, Object, int, CursorContext)}
     * @param searchResult search result
     * @return whether the searched key was found.
     */
    static boolean isHit(int searchResult) {
        return searchResult >= 0;
    }
}
