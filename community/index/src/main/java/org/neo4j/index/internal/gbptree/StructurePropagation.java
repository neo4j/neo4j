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
 * Means of communicating information about splits, caused by insertion, from lower levels of the tree up to parent
 * and potentially all the way up to the root. The content of StructurePropagation is acted upon when we are traversing
 * back up along the "traversal path" (see {@link org.neo4j.index.internal.gbptree}). Level where StructurePropagation
 * is evaluated is called "current level". Point of reference is mid child in current level and this is the parts
 * of interest:
 * <pre>
 *        ┌─────────┬──────────┐
 *        │ leftKey │ rightKey │
 *        └─────────┴──────────┘
 *       ╱          │           ╲
 * [leftChild]  [midChild]  [rightChild]
 *     ╱            │             ╲
 *    v             v              v
 * </pre>
 * <ul>
 *  <li> midChild - the child that was traversed to/through while traversing down the tree.
 *  <li> leftChild - left sibling to midChild.
 *  <li> rightChild - right sibling to midChild.
 *  <li> leftKey - if position of midChild in current level is {@code n} then leftKey is key at position {@code n-1}.
 *  If {@code n==0} then leftKey refer to leftKey in parent in a recursive manor.
 *  <li> rightKey - if position of midChild in current level is {@code n} then rightKey is the key at position {@code n}.
 *  If {@code n==keyCount} then rightKey to rightKey in parent in a recursive manor.
 * </ul>
 *
 * If position of {@link #midChild} {@code n > 0}.
 * <pre>
 * Current level-> [...,leftKey,rightKey,...]
 *                    ╱        │         ╲
 *              leftChild  midChild  rightChild
 *                  ╱          │           ╲
 *                 v           v            v
 * Child nodes-> [...] <───> [...] <────> [...]
 * </pre>
 *
 * If position of {@link #midChild} {@code n == 0}.
 * <pre>
 *
 * Parent node->          [...,leftKey,...]
 *                   ┌────────┘       └────────┐
 *                   v                         v
 * Current level-> [...] <───────────> [rightKey,...]
 *                     │               │        ╲
 *                 leftChild       midChild    rightChild
 *                     │               │          ╲
 *                     v               v           v
 * Child nodes->     [...] <───────> [...] <───> [...]
 * </pre>
 *
 * * If position of {@link #midChild} {@code n == keyCount}.
 * <pre>
 *
 * Parent node->                  [...,rightKey,...]
 *                           ┌────────┘       └───────┐
 *                           v                        v
 * Current level->    [...,leftKey] <─────────────> [...]
 *                       /        │                 |
 *                 leftChild  midChild          rightChild
 *                     /          │                 |
 *                    v           v                 v
 * Child nodes->   [...] <────> [...] <─────────> [...]
 * </pre>
 * @param <KEY> type of key.
 */
public class StructurePropagation<KEY> {
    /* <CONTENT> */
    // Below are the "content" of structure propagation
    /**
     * See {@link #keyReplaceStrategy}.
     */
    final KEY leftKey;

    /**
     * See {@link #keyReplaceStrategy}.
     */
    public final KEY rightKey;

    /**
     * See {@link #keyReplaceStrategy}.
     */
    final KEY bubbleKey;

    /**
     * New version of left sibling to mid child.
     */
    long leftChild;

    /**
     * New version of the child that was traversed to/through while traversing down the tree.
     */
    long midChild;

    /**
     * New right sibling to {@link #midChild}, depending on {@link #hasRightKeyInsert} this can be simple replace of an insert.
     */
    long rightChild;
    /* </CONTENT> */

    /* <ACTIONS> */
    // Below are the actions, deciding what the content of structure propagation should be used for.
    /**
     * Left child pointer needs to be replaced by {@link #leftChild}.
     */
    boolean hasLeftChildUpdate;

    /**
     * Right child pointer needs to be replaced by {@link #rightChild} OR, if {@link #hasRightKeyInsert} is true
     * {@link #rightChild} should be inserted as a completely new additional child, moving old right child to the right.
     */
    boolean hasRightChildUpdate;

    /**
     * Mid child pointer needs to be replaced by {@link #midChild}.
     */
    boolean hasMidChildUpdate;

    /**
     * {@link #rightKey} should be inserted at right keys position (not replacing old right key).
     */
    boolean hasRightKeyInsert;
    /* </ACTIONS> */

    /**
     * Depending on keyReplaceStrategy either {@link KeyReplaceStrategy#REPLACE replace} left / right key with
     * {@link #leftKey} / {@link #rightKey} or replace left / right key by {@link #bubbleKey} (with strategy
     * {@link KeyReplaceStrategy#BUBBLE bubble} rightmost from subtree). In the case of bubble, {@link #leftKey} / {@link #rightKey}
     * is used to find "common ancestor" of leaves involved in merge. See {@link org.neo4j.index.internal.gbptree}.
     */
    public KeyReplaceStrategy keyReplaceStrategy;

    boolean hasLeftKeyReplace;
    public boolean hasRightKeyReplace;

    StructurePropagation(KEY leftKey, KEY rightKey, KEY bubbleKey) {
        this.leftKey = leftKey;
        this.rightKey = rightKey;
        this.bubbleKey = bubbleKey;
    }

    /**
     * Clear booleans indicating change has occurred.
     */
    void clear() {
        hasLeftChildUpdate = false;
        hasRightChildUpdate = false;
        hasMidChildUpdate = false;
        hasRightKeyInsert = false;
        hasLeftKeyReplace = false;
        hasRightKeyReplace = false;
    }

    boolean isEmpty() {
        return !hasLeftChildUpdate
                && !hasRightChildUpdate
                && !hasMidChildUpdate
                && !hasRightKeyInsert
                && !hasLeftKeyReplace
                && !hasRightKeyReplace;
    }

    public interface StructureUpdate {
        void update(StructurePropagation structurePropagation, long childId);
    }

    public static final StructureUpdate UPDATE_LEFT_CHILD = (sp, childId) -> {
        sp.hasLeftChildUpdate = true;
        sp.leftChild = childId;
    };

    public static final StructureUpdate UPDATE_MID_CHILD = (sp, childId) -> {
        sp.hasMidChildUpdate = true;
        sp.midChild = childId;
    };

    public static final StructureUpdate UPDATE_RIGHT_CHILD = (sp, childId) -> {
        sp.hasRightChildUpdate = true;
        sp.rightChild = childId;
    };

    public enum KeyReplaceStrategy {
        REPLACE,
        BUBBLE
    }
}
