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

import org.neo4j.io.pagecache.PageCursor;

import static org.neo4j.index.internal.gbptree.TreeNode.NO_NODE_FLAG;

/**
 * Used to verify a chain of siblings starting with leftmost node.
 * Call {@link #assertNext(PageCursor, long, long, long, long, long)} with cursor pointing at sibling expected
 * to be right sibling to previous call to verify that they are indeed linked together correctly.
 * <p>
 * When assertNext has been called on node that is expected to be last in chain, use {@link #assertLast()} to verify.
 */
class RightmostInChain
{
    private long currentRightmost = TreeNode.NO_NODE_FLAG;
    private long expectedNextRightmost = TreeNode.NO_NODE_FLAG;
    private long expectedNextRightmostGen;
    private long currentRightmostGen;

    void assertNext( PageCursor cursor, long gen,
            long leftSibling, long leftSiblingGen, long rightSibling, long rightSiblingGen )
    {
        long pageId = cursor.getCurrentPageId();

        // Assert we have reached expected node and that we agree about being siblings
        if ( leftSibling != currentRightmost )
        {
            cursor.setCursorException( "Sibling pointer does align with tree structure. Expected left sibling to be " +
                    currentRightmost + " but was " + leftSibling );
        }
        if ( !(leftSiblingGen <= currentRightmostGen || currentRightmost == NO_NODE_FLAG) )
        {
            cursor.setCursorException( "Sibling pointer gen differs from expected. Expected left sigling gen to be " +
                    currentRightmostGen + ", but was " + leftSiblingGen );
        }
        if ( !(pageId == expectedNextRightmost ||
                (expectedNextRightmost == NO_NODE_FLAG && currentRightmost == NO_NODE_FLAG)) )
        {
            cursor.setCursorException( "Sibling pointer does not align with tree structure. Expected right sibling to be " +
                    expectedNextRightmost + " but was " + pageId );
        }
        if ( !(gen <= expectedNextRightmostGen || expectedNextRightmost == NO_NODE_FLAG) )
        {
            cursor.setCursorException(
                "Sibling pointer gen differs from expected. Expected right sigling gen to be " +
                expectedNextRightmostGen + ", but was " + gen );
        }

        // Update currentRightmost = pageId;
        currentRightmost = pageId;
        currentRightmostGen = gen;
        expectedNextRightmost = rightSibling;
        expectedNextRightmostGen = rightSiblingGen;
    }

    void assertLast()
    {
        assert expectedNextRightmost == NO_NODE_FLAG : "Expected rightmost right sibling to be " + NO_NODE_FLAG
                + " but was " + expectedNextRightmost;
    }
}
