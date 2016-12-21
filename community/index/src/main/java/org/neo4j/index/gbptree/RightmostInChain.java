/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.index.gbptree;

import org.neo4j.io.pagecache.PageCursor;

import static org.neo4j.index.gbptree.GenSafePointerPair.pointer;
import static org.neo4j.index.gbptree.TreeNode.NO_NODE_FLAG;

/**
 * Used to verify a chain of siblings starting with leftmost node.
 * Call {@link #assertNext(PageCursor)} with cursor pointing at sibling expected to be right sibling to previous call
 * to verify that they are indeed linked together correctly.
 * <p>
 * When assertNext has been called on node that is expected to be last in chain, use {@link #assertLast()} to verify.
 */
class RightmostInChain<KEY>
{
    private final TreeNode<KEY,?> node;
    private final long stableGeneration;
    private final long unstableGeneration;

    private long currentRightmost;
    private long expectedNextRightmost;
    private long expectedNextRightmostGen;
    private long currentRightmostGen;

    RightmostInChain( TreeNode<KEY,?> node, long stableGeneration, long unstableGeneration )
    {
        this.node = node;
        this.stableGeneration = stableGeneration;
        this.unstableGeneration = unstableGeneration;
        this.currentRightmost = TreeNode.NO_NODE_FLAG;
        this.expectedNextRightmost = TreeNode.NO_NODE_FLAG;
    }

    long assertNext( PageCursor cursor )
    {
        long pageId = cursor.getCurrentPageId();

        long leftSibling = node.leftSibling( cursor, stableGeneration, unstableGeneration );
        long rightSibling = node.rightSibling( cursor, stableGeneration, unstableGeneration );
        long leftSiblingGen = node.pointerGen( cursor, leftSibling );
        long rightSiblingGen = node.pointerGen( cursor, rightSibling );
        leftSibling = pointer( leftSibling );
        rightSibling = pointer( rightSibling );
        long gen = node.gen( cursor );

        // Assert we have reached expected node and that we agree about being siblings
        assert leftSibling == currentRightmost :
                "Sibling pointer does align with tree structure. Expected left sibling to be " +
                currentRightmost + " but was " + leftSibling;
        assert leftSiblingGen <= currentRightmostGen || currentRightmost == NO_NODE_FLAG:
                "Sibling pointer gen differs from expected. Expected left sigling gen to be " +
                currentRightmostGen + ", but was " + leftSiblingGen;
        assert pageId == expectedNextRightmost ||
               (expectedNextRightmost == NO_NODE_FLAG && currentRightmost == NO_NODE_FLAG) :
                "Sibling pointer does not align with tree structure. Expected right sibling to be " +
                expectedNextRightmost + " but was " + rightSibling;
        assert gen <= expectedNextRightmostGen || expectedNextRightmost == NO_NODE_FLAG:
                "Sibling pointer gen differs from expected. Expected right sigling gen to be " +
                expectedNextRightmostGen + ", but was " + gen;

        // Update currentRightmost = pageId;
        currentRightmost = pageId;
        currentRightmostGen = gen;
        expectedNextRightmost = rightSibling;
        expectedNextRightmostGen = rightSiblingGen;
        return pageId;
    }

    void assertLast()
    {
        assert expectedNextRightmost == NO_NODE_FLAG : "Expected rightmost right sibling to be " +
                                                       NO_NODE_FLAG + " but was " + expectedNextRightmost;
    }
}
