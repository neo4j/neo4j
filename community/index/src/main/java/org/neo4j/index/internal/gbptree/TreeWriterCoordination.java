/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * Coordinator to get feedback about and affect tree traversal and writing in {@link InternalTreeLogic}.
 * Apart from {@link #mustStartFromRoot()}, methods that return {@code boolean} may affect the traversal down the tree
 * to finally perform a leaf operation. They have a veto where returning {@code false} will let {@link InternalTreeLogic} unwind the traversal back up
 * to the root, go into {@link #flipToPessimisticMode() pessimistic} mode and traverse down again, with the pessimistic mode.
 */
interface TreeWriterCoordination
{
    /**
     * @return whether it's required that traversal starts from the root or not for each operation.
     */
    boolean mustStartFromRoot();

    /**
     * Called before every top-level insert/merge operation.
     */
    void initialize();

    /**
     * Called before traversing to a child node.
     * @param treeNodeId the tree node id which the traversal will go to.
     * @param childPos the child position in the parent which held this child pointer
     */
    void beforeTraversingToChild( long treeNodeId, int childPos );

    /**
     * Called after having traversed to a child node, the one which was specified in the most recent call to {@link #beforeTraversingToChild(long, int)}.
     * @param isInternal whether or not the node is an internal node.
     * @param availableSpace how much space is available in this node.
     * @param isStable whether this node is in stable generation. If it's not in stable generation then a change to it will need to create a successor.
     * @return {@code true} if operation is permitted, otherwise {@code false}.
     */
    boolean arrivedAtChild( boolean isInternal, int availableSpace, boolean isStable, int keyCount );

    /**
     * Called before splitting the leaf.
     * @param bubbleEntrySize size in bytes of the key which will be bubbled up to the parent.
     * @return {@code true} if operation is permitted, otherwise {@code false}.
     */
    boolean beforeSplittingLeaf( int bubbleEntrySize );

    /**
     * Called before removing entry from leaf.
     * @param sizeOfLeafEntryToRemove total size of the entry to remove.
     * @return {@code true} if operation is permitted, otherwise {@code false}.
     */
    boolean beforeRemovalFromLeaf( int sizeOfLeafEntryToRemove );

    /**
     * Called before a split of an internal node.
     * @param treeNodeId internal tree node id to split.
     */
    void beforeSplitInternal( long treeNodeId );

    /**
     * Called before under-flowing a leaf.
     * @param treeNodeId tree node id to underflow, i.e. rebalance or merge with siblings.
     */
    void beforeUnderflowInLeaf( long treeNodeId );

    /**
     * Ends the previously {@link #initialize() initialized} traversal,
     */
    void reset();

    /**
     * Flips to pessimistic mode. This will force all methods to take extra caution and never end up in a situation
     * where it will be necessary to return {@code false}.
     */
    void flipToPessimisticMode();

    /**
     * Does nothing and has no requirement of starting from the root every time.
     */
    TreeWriterCoordination NO_COORDINATION = new TreeWriterCoordination()
    {
        @Override
        public boolean mustStartFromRoot()
        {
            return false;
        }

        @Override
        public void initialize()
        {
        }

        @Override
        public boolean beforeSplittingLeaf( int bubbleEntrySize )
        {
            return true;
        }

        @Override
        public boolean arrivedAtChild( boolean isInternal, int availableSpace, boolean isStable, int keyCount )
        {
            return true;
        }

        @Override
        public void beforeTraversingToChild( long treeNodeId, int childPos )
        {
        }

        @Override
        public boolean beforeRemovalFromLeaf( int sizeOfLeafEntryToRemove )
        {
            return true;
        }

        @Override
        public void beforeSplitInternal( long treeNodeId )
        {
        }

        @Override
        public void beforeUnderflowInLeaf( long treeNodeId )
        {
        }

        @Override
        public void reset()
        {
        }

        @Override
        public void flipToPessimisticMode()
        {
        }
    };
}
