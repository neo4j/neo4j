/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.index.internal.gbptree;

/**
 * Means of communicating information about splits, caused by insertion, from lower levels of the tree up to parent
 * and potentially all the way up to the root.
 * <ul>
 *  <li> {@link #midChild} - new version of the child that was traversed to/through while traversing down the tree.
 *  <li> {@link #leftChild} - new version of left sibling to {@link #midChild}.
 *  <li> {@link #rightChild} - new right sibling to {@link #midChild} (not new version, completely new right sibling).
 *  <li> {@link #leftKey} - if position of {@link #midChild} pointer in node is {@code n} then {@link #leftKey} should
 *  replace key at position {@code n-1}. If {@code n==0} then {@link #leftKey} does not fit here and must be passed
 *  along one level further up the tree.
 *  <li> {@link #rightKey} - new key to be inserted at position {@code n} (if position of {@link #midChild} is
 *  {@code n}) together with {@link #rightChild}.
 * </ul>
 * If position of {@link #midChild} {@code n > 0}.
 * <pre>
 * Current level-> [...,leftKey,rightKey,...]
 *                    ╱        │         ╲
 *              leftChild  midChild  rightChild
 *                  ╱          │           ╲
 *                 v           v            v
 * Child nodes-> [...] <───> [...] <────> [...]
 * </pre>
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
 *
 * </pre>
 * @param <KEY> type of key.
 */
class StructurePropagation<KEY>
{
    // First level of updates, used when bubbling up the tree
    boolean hasLeftChildUpdate;
    boolean hasRightChildUpdate;
    boolean hasMidChildUpdate;
    boolean hasRightKeyInsert;
    boolean hasLeftKeyReplace;
    boolean hasRightKeyReplace;
    final KEY leftKey;
    final KEY rightKey;
    final KEY bubbleKey;
    long leftChild;
    long midChild;
    long rightChild;
    KeyReplaceStrategy keyReplaceStrategy;

    StructurePropagation( KEY leftKey, KEY rightKey, KEY bubbleKey )
    {
        this.leftKey = leftKey;
        this.rightKey = rightKey;
        this.bubbleKey = bubbleKey;
    }

    /**
     * Clear booleans indicating change has occurred.
     */
    void clear()
    {
        hasLeftChildUpdate = false;
        hasRightChildUpdate = false;
        hasMidChildUpdate = false;
        hasRightKeyInsert = false;
        hasLeftKeyReplace = false;
        hasRightKeyReplace = false;
    }

    interface StructureUpdate
    {
        void update( StructurePropagation structurePropagation, long childId );
    }

    static final StructureUpdate UPDATE_LEFT_CHILD = ( sp, childId ) ->
    {
        sp.hasLeftChildUpdate = true;
        sp.leftChild = childId;
    };

    static final StructureUpdate UPDATE_MID_CHILD = ( sp, childId ) ->
    {
        sp.hasMidChildUpdate = true;
        sp.midChild = childId;
    };

    static final StructureUpdate UPDATE_RIGHT_CHILD = ( sp, childId ) ->
    {
        sp.hasRightChildUpdate = true;
        sp.rightChild = childId;
    };

    enum KeyReplaceStrategy
    {
        REPLACE, BUBBLE
    }
}
